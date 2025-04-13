package ComDB

import (
	"ComDB/data"
	"ComDB/fio"
	"ComDB/index"
	"ComDB/utils"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	_ "go/ast"
	_ "gopkg.in/check.v1"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const seqNoKey = "seq-no-key"

// DB bitcask db instance
type DB struct {
	mu          *sync.RWMutex
	activeFile  *data.DataFile            // 当前活跃文件
	olderFile   map[uint32]*data.DataFile // 非活跃文件-> 只用于读
	fileIds     []int                     // 只能在加载索引的时候使用
	options     Options                   // 数据库配置
	index       index.Indexer
	seqNo       uint64       // 当前事务的序列号，事务的序列号是全局递增的
	isMerging   bool         // 是否当前正在进行merge操作
	seqNoExist  bool         // 是否存在序列号文件--> 这个文件是在数据库关闭的时候才会生成
	isInitial   bool         // 是否是第一次初始化
	fileLock    *flock.Flock //目录锁
	reclaimSize int64        // 统计有多少数据是可以用于merge的
	update      *Update
}

type Stat struct {
	KeyNum          uint  // key 总数
	DataFile        uint  // 磁盘上数据文件数量
	ReclaimableSize int64 // 可以进行merge回收的数据量
	DiskSize        int64 // 数据目录所占磁盘空间
}

func Open(options Options) (*DB, error) {
	// 对用户传入的数据进行校验
	if err := checkOption(options); err != nil {
		return nil, err
	}
	var isInitial bool

	// 判断数据目录是否存在
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 加上文件锁
	fileLock := flock.New(filepath.Join(options.DirPath, data.FileLockName))
	lock, err2 := fileLock.TryLock()
	if err2 != nil {
		return nil, err2
	}
	if !lock {
		return nil, ErrDatabaseIsUsing
	}

	dir, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(dir) == 0 {
		isInitial = true
	}

	// 初始化db
	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		olderFile: make(map[uint32]*data.DataFile),
		index:     index.NewIndexer(options.IndexerType, options.DirPath, options.SyncWrite),
		isInitial: isInitial,
		fileLock:  fileLock,
		update:    nil,
	}
	// 加载merge数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	// 加载数据文件 => 其实就是将用户指定目录下的数据文件给放到数据库项中可以识别到文件标识符
	if err := db.loadDataFiles(); err != nil { // 现在hint-index还是有的
		return nil, err
	}
	// 如果是B +树的索引在NewIndexer的时候就已经把索引建好了
	if db.options.IndexerType != BPTree {
		// 查看目录中是否有hint文件，如果有就在这个目录下加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}
		// 从数据文件中加载索引的方法 => 在内存中建立BTree
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
		// 修改IO
		if err := db.resetIoType(); err != nil {
			return nil, err
		}
	}

	// 取出当前的事务序列号
	if options.IndexerType == BPTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		// 这个地方有一个细节就是如果没有从数据文件中加载索引是没法拿到当前活跃文件的最新偏移的
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WritOff = size
		}
	}
	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	// 释放目录锁
	defer func() {
		err := db.fileLock.Unlock()
		if err != nil {
			fmt.Printf("close file lock err")
			return
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 关闭index
	if err := db.index.Close(); err != nil {
		return err
	}
	// 保存当前的序列号给b+树索引进行加载
	if db.options.IndexerType == BPTree {
		seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath, fio.StandardFIO)
		if err != nil {
			return err
		}
		record := &data.LogRecord{
			Key:   []byte(seqNoKey),
			Value: []byte(strconv.FormatUint(db.seqNo, 10)),
		}
		enRecord, _ := data.EncodeLogRecord(record)
		if err := seqNoFile.Write(enRecord); err != nil {
			return err
		}
		if err := seqNoFile.Sync(); err != nil {
			return err
		}
	}

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	// 关闭旧的数据文件
	for _, file := range db.olderFile {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil

}

// Stat 返回数据库相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFile))
	if db.activeFile != nil {
		dataFiles += 1
	}
	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}

	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFile:        dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{data.FileLockName})
}

// Sync 持久化活跃文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeFile.Sync()

}

// Put 写入kv数据，key不为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		// 如果key为0就退出
		return ErrKeyIsEmpty
	}
	// 构造LogRecord结构体
	enKey := logRecordKeyWithSeq(key, nonTransactionSeqNo)
	logRecord := &data.LogRecord{
		// 非事务序列号，用于在解析的时候区分事务和非事务
		Key:   enKey,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// 拿到插入之后的内存索引信息
	logRecordPos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return ErrIndexUpdateFailed
	}
	// 更新内存索引
	if oldValue := db.index.Put(key, logRecordPos); oldValue != nil {
		db.reclaimSize += oldValue.Size
	}
	return nil
}

// 获取key的资源
func (db *DB) Get(key []byte) ([]byte, error) {
	// 注意一下，这里读数据是一定要加锁的保护的，内存索引是写并发安全，但读并不是
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	// 从内存数据接口中取出索引信息
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	// 使用文件ID找到对应的数据文件
	return db.getValueByPosition(logRecordPos)
}

// Delete 删除数据
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 查找key是否存在
	if pos := db.index.Get(key); pos == nil {
		return ErrKeyNotFound
	}
	// 构建LogRecord信息标识这个key被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted}
	// 将这条数据给插入到数据文件中去
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}
	db.reclaimSize += int64(pos.Size)

	// 删除在内存索引中的对应的数据
	oldValue, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldValue != nil {
		db.reclaimSize += oldValue.Size
	}
	return nil
}

// ListKey 将所有Key给列出来
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()

	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx += 1
	}
	return keys
}

// Fold 对数据库中的所有的数据进行指定的操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		// 将所有的值给取出来
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		// 执行函数，如果用户返回false那么就终止执行，这种写法我的确从来没有写过
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// getValueByPosition 根据索引信息获取value值
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	// 使用文件ID找到对应的数据文件
	var dataFile *data.DataFile
	if pos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFile[pos.Fid]
	}
	// 数据文件空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据内存中的偏移量去读取数据
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	// 判断logRecord的类型
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写log
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	// 判断现在的活跃文件是否存在
	// 为空就需要初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 写入编码
	encodeRes, size := data.EncodeLogRecord(logRecord)
	// 如果当前的数据 + 活跃文件的数据大小 > 阈值 => change status
	if db.activeFile.WritOff+size > db.options.DataFileSize {
		// 将当前文件的内容持久化一下
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 将当前的活跃文件转化为旧的数据文件
		db.olderFile[db.activeFile.FileId] = db.activeFile
		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 开始数据写入的操作了
	writeOff := db.activeFile.WritOff
	if err := db.activeFile.Write(encodeRes); err != nil {
		return nil, err
	}
	// 根据用户需求判断是否每次写入之后都进行持久化
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 构造一个内存索引信息对内存中的内容进行同步
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: size}
	return pos, nil
}

/*
这个地方要记住，只要涉及到文件的创建一定需要加锁
*/
func (db *DB) setActiveDataFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileId + 1
	}
	// 打开新的数据文件-> 具体往什么位置打开数据文件？-> 由用户来决定实例所在的目录
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

func checkOption(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is none")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("DataFileMergeRatio can only lie in 0~1")
	}
	return nil
}

// loadDataFiles 将数据库数据目录信息加载到DB中
func (db *DB) loadDataFiles() error {
	// 根据配置项将配置读取出来
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	// 遍历目录中的所有文件找到所有以data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 00001.data ---> 进行分割
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 现在就获得了所有的fileId的list了
	// 对文件ID进行排序，从小到大依次排序
	sort.Ints(fileIds)
	db.fileIds = fileIds
	// 遍历每一个文件ID打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.options.MMapAtStartUp {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			// 最后一个id是最大的说明是活跃文件
			db.activeFile = dataFile
		} else {
			db.olderFile[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
			db.reclaimSize += int64(pos.Size)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 如果比最近未参与 merge 的文件 id 更小，则说明已经从 Hint 文件中加载索引了
		if hasMerge && fileId < nonMergeFileId {
			// 说人话就是已经经过之前loadHintFile处理的文件
			continue
		}
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFile[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: size}

			// 解析 key，拿到事务序列号，在加载数据文件的时候要保证事务
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对应的 seq no 的数据可以更新到内存索引中
				// 用了一个专门的type来标识事务是否完成
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if int(seqNo) > currentSeqNo {
				currentSeqNo = int(seqNo)
			}

			// 递增 offset，下一次从新的位置开始读取
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WritOff = offset
		}
	}

	// 更新事务序列号, 所以获取事务的序列号是在获取索引的过程中获得的
	db.seqNo = uint64(currentSeqNo)
	return nil
}

// 这里就是专门用一个文件来保存B+树的序列号，如果是持久化索引不需要在内存中构建
func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath, fio.StandardFIO)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoExist = true
	return os.Remove(fileName)
}

func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.olderFile {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
