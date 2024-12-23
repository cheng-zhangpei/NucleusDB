package index

import (
	"ComDB/data"
	"github.com/stretchr/testify/assert"
	_ "github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Delete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("a"))
	assert.True(t, res4)

}

func TestBTree_Get(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res2)
	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
	t.Log(pos2)
}

func TestBTree_Put(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res2)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBtree()
	// 1.BTree 为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	//	2.BTree 有数据的情况
	bt1.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.有多条数据
	bt1.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("key = ", string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}
	t.Log("=========================================================")

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		t.Log("key = ", string(iter4.Key()))

		assert.NotNil(t, iter4.Key())
	}
	t.Log("=========================================================")

	// 4.测试 seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("eede")); iter5.Valid(); iter5.Next() { //找到第一个比eede大的数据
		assert.NotNil(t, iter5.Key())
		t.Log("key = ", string(iter5.Key()))

	}
	t.Log("=========================================================")

	// 5.反向遍历的 seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("eede")); iter6.Valid(); iter6.Next() {
		t.Log("key = ", string(iter6.Key()))

		assert.NotNil(t, iter6.Key())
	}
}
