package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(filename string) {
	if err := os.Remove(filename); err != nil {
		panic(err)
	}
}
func TestNewFileIOManager(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	defer destroyFile(filepath.Join("/tmp", "a.data"))

}
func TestFileIo_Write(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp", "a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	defer destroyFile(filepath.Join("/tmp", "a.data"))

	n, err1 := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err1)
}

func TestFileIo_Read(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("/tmp", "0002.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	defer destroyFile(filepath.Join("/tmp", "0002.data")) // 函数运行结束之后就退出就ok

	_, err2 := fio.Write([]byte("key-b"))
	assert.Nil(t, err2)

	b := make([]byte, 5) // 这个地方只会读出来五个数据
	n, err := fio.Read(b, 0)
	t.Log(b, n)
	assert.Equal(t, []byte("key-b"), b)
}
