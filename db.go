package ComDB

import (
	"ComDB/data"
	"ComDB/index"
	"errors"
	_ "go/ast"
	_ "gopkg.in/check.v1"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask db instance
type DB struct {
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃文件
	olderFile  map[uint32]*data.DataFile //  非活跃文件-> 只用于读
	fileIds    []int                     // 只能在加载索引的时候使用
	options    Options
	index      index.Indexer
}

func Open(options Options) (*DB, error) {
	// 对用户传入的数据进行校验
	if err := checkOption(options); err != nil {
		return nil, err
	}
	// 判断数据目录是否存在
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err == nil {
			return nil, err
		}
	}
	// 初始化db
	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		olderFile: make(map[uint32]*data.DataFile),
		index:     index.NewIndexer(options.IndexerType),
	}
	// 加载数据文件 => 其实就是将用户指定目录下的数据文件给放到数据库项中可以识别到文件标识符
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	// 从数据文件中加载索引的方法 => 在内存中建立BTree
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

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
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// 拿到插入之后的内存索引信息
	logRecordPos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	// 更新内存索引
	if ok := db.index.Put(key, logRecordPos); !ok {
		return ErrIndexUpdateFailed
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
		return nil
	}
	// 构建LogRecord信息标识这个key被删除的
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	// 将这条数据给插入到数据文件中去
	if _, err := db.appendLogRecord(logRecord); err != nil {
		return nil
	}
	// 删除在内存索引中的对应的数据
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// ListKey 将所有Key给列出来
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
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

// 追加写log
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
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
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
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
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID)
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
	return nil
}

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
	//现在就获得了所有的fileId的list了
	// 对文件ID进行排序，从小到大依次排序
	sort.Ints(fileIds)
	db.fileIds = fileIds
	// 遍历每一个文件ID打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
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

// loadIndexFromDataFiles 根据fileIds进行索引的初始化
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}
	// 遍历并取出文件的内容
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFile[fileId]
		}
		// 处理这个文件中的所有的内容

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				// 这个error不能直接返回，因为会存在文件读取结束的情况
				if err == io.EOF {
					break
				}
				return err
			}
			// 这个地方返回的只是地址要小心
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				// 将其从索引中删除
				ok := db.index.Delete(logRecord.Key)
				if !ok {
					return ErrIndexUpdateFailed
				}
			} else {
				ok := db.index.Put(logRecord.Key, logRecordPos)
				if !ok {
					return ErrIndexUpdateFailed
				}
			}

			offset = offset + size

		}
		// 如果当前文件是活跃文件，需要更新offset
		if i == len(db.fileIds)-1 {
			db.activeFile.WritOff = offset
		}
	}
	return nil
}
