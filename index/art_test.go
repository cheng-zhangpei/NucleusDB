package index

import (
	"ComDB/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewAdaptiveRadixTree()
	res1 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.NotNil(t, res1)
	res2 := art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.NotNil(t, res2)
	res3 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.NotNil(t, res3)
	t.Log(t)

}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewAdaptiveRadixTree()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	assert.NotNil(t, pos)

	pos1 := art.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1123, Offset: 990})
	pos2 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos2)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewAdaptiveRadixTree()

	ok1 := art.Delete([]byte("not exist"))
	assert.False(t, ok1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	ok2 := art.Delete([]byte("key-1"))
	assert.True(t, ok2)

	pos := art.Get([]byte("key-1"))
	assert.Nil(t, pos)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewAdaptiveRadixTree()

	assert.Equal(t, 0, art.Size())

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewAdaptiveRadixTree()

	art.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("adse"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bade"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
		t.Log(iter.Key())
		t.Log(iter.Value())
	}
	iter.Close()

}
