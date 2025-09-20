package NucleusDB

import (
	"NucleusDB/index"
	"bytes"
)

// Iterator 面向用户的迭代器
type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

// NewIterator 新建迭代器，这里注意一下是属于DB这个结构体的-> 这里新建的是用户的迭代器，用户的迭代器这里会对索引的迭代器进行封装
func (db *DB) NewIterator(opts IteratorOptions) Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return Iterator{
		db:        db,
		options:   opts,
		indexIter: indexIter,
	}
}

// Rewind 回到初始位置
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 找到第一个大于key的键的位置
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 跳转到下一个key.需要更具prefix来进行过滤
func (it *Iterator) Next() {

	it.indexIter.Next()
	it.skipToNext()
}

// Valid 是否有效，即是否已经遍历完所有的key
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 当前遍历位置的Key数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()

}

// Value 当前遍历位置的value数据
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	// 现在需要使用位置索引信息去找到value值
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器释放资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen < len(key) && bytes.Compare(key[:prefixLen], it.options.Prefix) == 0 {
			// 如果前缀是符合的话,就跳出去，不然就直到遍历完所有的key
			break
		}
	}
}
