package example

import (
	"ComDB"
	"fmt"
)

func main() {
	opts := ComDB.DefaultOptions
	opts.DirPath = "/tmp/bitcask-go"
	db, err := ComDB.Open(opts)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Printf(string(val))
}
