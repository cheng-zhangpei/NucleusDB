package NucleusDB

import (
	"os"
)

type Options struct {
	DirPath            string  // 数据库数据目录
	DataFileSize       int64   // 数据文件大小
	SyncWrite          bool    //是否每次写入都持久化
	IndexerType        int8    // 内存索引类型
	MMapAtStartUp      bool    // 是否采用内存映射加速数据库启动
	DataFileMergeRatio float32 //数据文件合并的阈值
	MaxWaterSize       uint64  // 最大的MVCC事务池中事务数量
}

type IteratorOptions struct {
	// 遍历前缀为指定前缀的Key
	Prefix []byte
	// 是否可逆
	Reverse bool
}

type ServerConfig struct {
	Host string
	Port string
}

type WriteBatchOptions struct {
	// 批次中最大数据量
	MaxBatchNum uint32
	// 提交事务的时候是否进行sync持久化
	SyncWrite bool
}
type CompressOptions struct {
	ComPressNumThreshold   int64   // 该记忆空间压缩
	CompressHighSimDis     float64 // 高相似压缩距离
	CompressCleanThreshold float64 // 高压缩清理系数
	CompressNum            int64   // 压缩批次中记忆节点数量
}

type CompressionModelOptions struct {
	Endpoint string // 模型地址
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
	MaxWaterSize:       5,
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

var DefaultWebServerOptions = ServerConfig{
	Host: "127.0.0.1",
	Port: "6380",
}

var DefaultCompressOptions = CompressOptions{
	ComPressNumThreshold:   3,   // 记忆空间可压缩节点数量达到 3 时触发压缩
	CompressHighSimDis:     0.8, // 相似度大于 0.8 的节点被认为是高度相似的
	CompressCleanThreshold: 0.2, // 压缩系数大于 0.2 的节点会被清理
	CompressNum:            3,   // 三个记忆节点达到压缩阈值的时候进行压缩
}

var DefaultCompressModelOptions = CompressionModelOptions{
	Endpoint: "http://172.24.216.71:5000/generate",
}
