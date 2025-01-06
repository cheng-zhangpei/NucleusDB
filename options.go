package ComDB

import "os"

type Options struct {
	DirPath            string  // 数据库数据目录
	DataFileSize       int64   // 数据文件大小
	SyncWrite          bool    //是否每次写入都持久化
	IndexerType        int8    // 内存索引类型
	MMapAtStartUp      bool    // 是否采用内存映射加速数据库启动
	DataFileMergeRatio float32 //数据文件合并的阈值

}

type IteratorOptions struct {
	// 遍历前缀为指定前缀的Key
	Prefix []byte
	// 是否可逆
	Reverse bool
}

type WriteBatchOptions struct {
	// 批次中最大数据量
	MaxBatchNum uint32
	// 提交事务的时候是否进行sync持久化
	SyncWrite bool
}

type IndexerType = int8

const (
	BTree IndexerType = iota + 1
	ARTree
	BPTree
)

// DefaultOptions 一个默认的options
var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024,
	SyncWrite:          true,
	IndexerType:        BTree,
	MMapAtStartUp:      true,
	DataFileMergeRatio: 0.3,
}

// DefaultIteratorOptions 一个默认的索引迭代器
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 32,
	SyncWrite:   false,
}
