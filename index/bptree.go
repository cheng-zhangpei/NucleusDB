package index

import (
	"ComDB/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree b + tree 磁盘索引
type BPlusTree struct {
	tree *bbolt.DB // 这个本质上就是一个DB的实例
}

// NewBPlusTree 初始化B+Tree的索引
// 打开数据库实例，打开bucket去读写数据
// 注意一下这些方法一定是要使用事务进行提交

func NewBPlusTree(dirPath string, syncWrite bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	// 这里一定要小心
	opts.NoSync = !syncWrite
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic(err)
	}
	// 我感觉很多数据引擎的事务似乎都是用Update来进行封装的
	err = bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	})
	if err != nil {
		panic("failed to create bucket: " + err.Error())
	}
	return &BPlusTree{
		tree: bptree,
	}

}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}
func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool = false
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete entry: " + err.Error())
	}
	return ok
}

func (bpt *BPlusTree) Size() int {
	var value int = 0
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get entry: " + err.Error())
	}
	return value
}

// Close 这个地方一定要记住关闭实例
func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// ====================================================迭代器=========================================

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

type bptreeIterator struct {
	tx       *bbolt.Tx
	cursor   *bbolt.Cursor
	reverse  bool
	curKey   []byte
	curValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin transaction: " + err.Error())
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

// Rewind 重新回到迭代器起点
func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Last()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.First()
	}
}

// Seek 根据传入的key查找到第一个大于或者小于等于目标Key，从这个Key开始遍历
func (bpi *bptreeIterator) Seek(key []byte) {
	// 这个迭代器的写法还是蛮复杂的
	bpi.curKey, bpi.curValue = bpi.cursor.Seek(key)
}

// Next 跳转到下一个key
func (bpi *bptreeIterator) Next() {
	if !bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Next()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.Prev()
	}
}

// Valid 是否有效，即是否已经遍历完所有的key
func (bpi *bptreeIterator) Valid() bool {
	// 验证当前的key是否为空就可以了
	return len(bpi.curKey) != 0
}

// Key 当前遍历位置的Key数据
func (bpi *bptreeIterator) Key() []byte {
	return bpi.curKey
}

// Value 当前遍历位置的value数据
func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.curValue)
}

// Close 关闭迭代器释放资源
func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()

}
