package index

import (
	"ComDB/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 内存抽象接口定义
type Indexer interface {
	// Pur put向索引中存储的key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 根据key取出信息
	Get(key []byte) *data.LogRecordPos
	// Delete 删除信息
	Delete(key []byte) bool
	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
	// Size 判断索引中存在多少条数据
	Size() int
}

type IndexType = int8

// 运行有多个系统索引
const (
	// Btree index
	Btree IndexType = iota + 1
	// ART ART自适应基数树
	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBtree()
	case ART:
		// todo 自适应基数树
		return nil
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
type Iterator interface {
	Rewind()                   // 重新回到迭代器起点
	Seek(key []byte)           // 根据传入的key查找到第一个大于或者小于等于目标Key，从这个Key开始遍历
	Next()                     // 跳转到下一个key
	Valid() bool               // 是否有效，即是否已经遍历完所有的key
	Key() []byte               // 当前遍历位置的Key数据
	Value() *data.LogRecordPos // 当前遍历位置的value数据
	Close()                    // 关闭迭代器释放资源
}
