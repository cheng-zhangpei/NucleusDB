package index

import (
	"ComDB/data"
	"bytes"
	"github.com/google/btree"
	_ "github.com/google/btree"
	"sort"
	"sync"
)

// BTree BTree索引
type BTree struct {
	tree *btree.BTree // 注意一下BTree的写操作是并发不安全的
	lock *sync.RWMutex
}

// ------ 下面三个接口的实现主要是需要调用BTree的功能 --------

// NewBtree 初始化
func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: &sync.RWMutex{},
	}
}
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it) // 由于写入不安全这个位置需要加锁
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}
func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return NewBtreeIterator(bt.tree, reverse)
}
func (bt *BTree) Size() int {
	return bt.tree.Len()
}
func (bt *BTree) Close() error {
	return nil
}

// BTree 索引迭代器
type BtreeIterator struct {
	currIndex int     // 当前的位置
	reverse   bool    // 是否为反向的遍历
	values    []*Item // key 位置索引的信息
}

// todo: 这个地方有一个问题就是数据结构BTree的迭代器是不能满足我们的需求的，所以我们需要把数据全部从数据结构里面取出来，这会导致内存膨胀（Double time）
func NewBtreeIterator(tree *btree.BTree, reverse bool) *BtreeIterator {
	var idx int
	values := make([]*Item, tree.Len())
	// 将所有的数据都放到Values数组里面去
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &BtreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器起点
func (bTi *BtreeIterator) Rewind() {
	bTi.currIndex = 0
}

// Seek 根据传入的key查找到第一个大于或者小于等于目标Key，从这个Key开始遍历
func (bTi *BtreeIterator) Seek(key []byte) {
	// 这个迭代器的写法还是蛮复杂的
	if bTi.reverse {
		bTi.currIndex = sort.Search(len(bTi.values), func(i int) bool {
			return bytes.Compare(bTi.values[i].key, key) >= 0
		})
	} else {
		bTi.currIndex = sort.Search(len(bTi.values), func(i int) bool {
			return bytes.Compare(bTi.values[i].key, key) <= 0
		})
	}

}

// Next 跳转到下一个key
func (bTi *BtreeIterator) Next() {
	bTi.currIndex++
}

// Valid 是否有效，即是否已经遍历完所有的key
func (bTi *BtreeIterator) Valid() bool {
	return bTi.currIndex < len(bTi.values)
}

// Key 当前遍历位置的Key数据
func (bTi *BtreeIterator) Key() []byte {
	return bTi.values[bTi.currIndex].key
}

// Value 当前遍历位置的value数据
func (bTi *BtreeIterator) Value() *data.LogRecordPos {
	return bTi.values[bTi.currIndex].pos
}

// Close 关闭迭代器释放资源
func (bTi *BtreeIterator) Close() {
	bTi.values = nil
}
