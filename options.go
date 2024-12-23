package ComDB

import "os"

type Options struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64  // 数据文件大小
	SyncWrite    bool   //是否每次写入都持久化
	IndexerType  int8   // 内存索引类型
}

type IteratorOptions struct {
	// 遍历前缀为指定前缀的Key
	Prefix []byte
	// 是否可逆
	Reverse bool
}
type IndexerType = int8

const (
	BTree IndexerType = iota + 1
	ARTree
)

// DefaultOptions 一个默认的options
var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrite:    true,
	IndexerType:  BTree,
}

// DefaultIteratorOptions 一个默认的索引迭代器
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
