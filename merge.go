package ComDB

import (
	"ComDB/data"
	"ComDB/fio"
	"ComDB/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const mergeDirName = "-merge"
const mergeFinishedKey = "merge.finished"

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	// 同一个时刻只能有一个merge例程，这里其实就是用一个标识位
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProcessing
	}
	// 查看merge是否到达用户设置的阈值
	totalSize, err2 := utils.DirSize(db.options.DirPath)
	if err2 != nil {
		db.mu.Unlock()
		return err2
	}

	if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreachable
	}
	// 查看剩余磁盘空间是否还能支撑merge
	availableDiskSize, err := utils.AvailableDiskSize()

	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) > -availableDiskSize {
		db.mu.Unlock()
		return ErrMergeRatioUnreachable
	}
	// 开始merge
	db.isMerging = true
	// 这种写法要稍微注意一下
	defer func() {
		db.isMerging = false
	}()

	// 持久化写活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 将活跃文件转化为旧文件
	db.olderFile[db.activeFile.FileId] = db.activeFile
	// 打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}

	nonMergeFileId := db.activeFile.FileId

	// 现在已经获得的所有需要进行merge的活跃文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFile {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// 将merge文件 从小到大 排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	mergePath := db.getMergePath()
	// 如果这个merge目录存在（说明之前存在merge）
	// 需要将目录删掉
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.Remove(mergePath); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 在该目录下打开bitcask实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrite = false

	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}
	// 打开一个hint文件处理索引
	ioType := fio.StandardFIO
	hintFile, err := data.OpenHintDataFile(mergePath, ioType)
	if err != nil {
		return err
	}
	if hintFile == nil {
		ioManager, err := fio.NewIOManager(filepath.Join(mergePath, data.HintFileName), ioType)
		if err != nil {
			return err
		}
		hintFile = &data.DataFile{
			FileId:    0,
			WritOff:   0,
			IOManager: ioManager,
		}
	}
	// 遍历每一个数据文件，此处记住一下，merge文件并不只是一个呀
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析拿到实际的 key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// 和内存中的索引位置进行比较，如果有效则重写
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset {
				// 清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 将当前位置索引写到 Hint 文件当中
				if err := hintFile.WriteHintFile(realKey, pos); err != nil {
					return err
				}
			}
			// 增加 offset
			offset += size
		}
	}
	// 对当前文件持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	// 写merge完成的标识
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath, ioType)
	if err != nil {
		return err
	}
	mergeFinishedLogRecord := &data.LogRecord{
		Key: []byte(mergeFinishedKey),
		// 在最后这个merge标识中可以记录me
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	// 编码之后就返回相应的字节流
	encLogRecord, _ := data.EncodeLogRecord(mergeFinishedLogRecord)
	if err := mergeFinishedFile.Write(encLogRecord); err != nil {
		return err
	}
	// 对写完的数据进行持久化
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

// /tmp/bitcask
// tmp/bitcask-merge
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath) // 得到目录的名称
	return filepath.Join(dir, base+mergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// 如果目录不存在
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	// merge 文件读取之后需要删除目录
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	// 查找标识merge完成的文件，判断merge是否处理完了
	var mergeFinished bool = false
	var mergeFileName []string
	for _, entry := range dirEntries {
		// 判断merge在这个目录中是否完成， merge完成之后会在目录中放一个merge完成的文件标志
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		if entry.Name() == data.FileLockName {
			continue
		}
		mergeFileName = append(mergeFileName, entry.Name())
	}
	// 没有merge标识符
	if !mergeFinished {
		return nil
	}
	// 标识没有完成merge的文件id
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	// 将旧的数据文件删掉(删除比nonMergeFileId更小的数据文件)
	var fileId uint32 = 0
	// 删除db中已经merge的datafile
	for ; fileId < nonMergeFileId; fileId++ {
		// 获取数据文件的名称并将后缀去掉转化为整数
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			// 数据如果存在就删除掉
			err := os.Remove(fileName)
			if err != nil {
				return err
			}
		}
	}

	// 将新的数据文件（merge之后）移动到数据目录中去，这里可能会有多个merge之后的数据文件
	for _, fileName := range mergeFileName {
		srcPath := filepath.Join(mergePath, fileName)
		desPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, desPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	var ioType = fio.StandardFIO
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath, ioType)
	if err != nil {
		return 0, err
	}
	// 只有一条记录则偏移为0
	logRecord, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	// 这个value是id？
	nonMergeId, err := strconv.Atoi(string(logRecord.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeId), nil

}

// 从hint文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	ioType := fio.StandardFIO
	hintFile, err := data.OpenHintDataFile(db.options.DirPath, ioType)
	if err != nil {
		return err
	}
	if hintFile == nil {
		return nil
	}
	// 读取文件的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		// 解码拿到位置位置索引信息--> 需要再次明确一下每次插入索引的时候是需要用字节来进行插入的
		pos := data.DecodeLogRecordPos(logRecord.Value)
		// 拿到位置索引信息之后不断地往B+ index中进行插入
		db.index.Put(logRecord.Key, pos)
		// 偏移到下一条数据
		offset += size
	}
	return nil
}
