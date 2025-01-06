package index

import (
	"ComDB/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// 自适应基数树索引
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

type ARTIterator struct {
	currIndex int     // 当前的位置
	reverse   bool    // 是否为反向的遍历
	values    []*Item // key 位置索引的信息
}

func NewAdaptiveRadixTree() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// Put Pur put向索引中存储的key对应的数据位置信息
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	value, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if value == nil {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// Get 根据key取出信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock() // 读取锁
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// Delete 删除信息
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

// Iterator 索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	// 和之前一样的思路就是把数据用数字升序或者是降序排列
	if art.tree == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()
	return NewARTIterator(art.tree, reverse)
}
func (art *AdaptiveRadixTree) Close() error {
	return nil
}

func NewARTIterator(art goart.Tree, reverse bool) *ARTIterator {
	var idx int
	values := make([]*Item, art.Size())
	// 将所有的数据都放到Values数组里面去
	if reverse {
		idx = art.Size() - 1
	} else {
		idx = 0
	}
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	art.ForEach(saveValues)
	return &ARTIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Size 判断索引中存在多少条数据
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

// Rewind 重新回到迭代器起点
func (ai *ARTIterator) Rewind() {
	ai.currIndex = 0
}

// Seek 根据传入的key查找到第一个大于或者小于等于目标Key，从这个Key开始遍历
func (ai *ARTIterator) Seek(key []byte) {
	// 这个迭代器的写法还是蛮复杂的
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	}

}

// Next 跳转到下一个key
func (ai *ARTIterator) Next() {
	ai.currIndex++
}

// Valid 是否有效，即是否已经遍历完所有的key
func (ai *ARTIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

// Key 当前遍历位置的Key数据
func (ai *ARTIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

// Value 当前遍历位置的value数据
func (ai *ARTIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

// Close 关闭迭代器释放资源
func (ai *ARTIterator) Close() {
	ai.values = nil
}
