package ComDB

import (
	"os"
	"testing"
)

// 测试MVCC的基本功能

func Test_base_mvcc(test *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	if err != nil {
		test.Fatal(err)
	}
	db.Update()
}
