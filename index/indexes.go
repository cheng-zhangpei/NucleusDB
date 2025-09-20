package index

import (
	"NucleusDB/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 内存抽象接口定义
type Indexer interface {
	// Put Pur put向索引中存储的key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	// Get 根据key取出信息
	Get(key []byte) *data.LogRecordPos
	// Delete 删除信息
	Delete(key []byte) (*data.LogRecordPos, bool)
	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
	// Size 判断索引中存在多少条数据
	Size() int
	// Close 关闭索引
	Close() error
}

type IndexType = int8

// 运行有多个系统索引
const (
	// Btree index
	Btree IndexType = iota + 1
	// ART ART自适应基数树
	ART
	// b+树
	BPTree
)

func NewIndexer(typ IndexType, dirPath string, syncWrite bool) Indexer {
	switch typ {
	case Btree:
		return NewBtree()
	case ART:
		return NewAdaptiveRadixTree()
	case BPTree:
		return NewBPlusTree(dirPath, syncWrite)
	default:
		panic("unhandled default case")
	}
}

// Item 内存索引中的结构
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器接口：主要是为了兼容不同的索引的类型
// todo 细粒度索引避免竞争同一个索引的，多索引之间的负载均衡

type Iterator interface {
	// Rewind 重新回到迭代器起点
	Rewind()
	// Seek 根据传入的key查找到第一个大于或者小于等于目标Key，从这个Key开始遍历
	Seek(key []byte)
	// Next 跳转到下一个key
	Next()
	// Valid 是否有效，即是否已经遍历完所有的key
	Valid() bool
	// Key 当前遍历位置的Key数据
	Key() []byte
	// Value 当前遍历位置的value数据
	Value() *data.LogRecordPos
	// Close 关闭迭代器释放资源
	Close()
}
