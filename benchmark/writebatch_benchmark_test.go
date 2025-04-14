package benchmark

import (
	"ComDB"
	"ComDB/utils"
	"testing"
)

func Benchmark_SerialShortTxn(b *testing.B) {
	opts := ComDB.DefaultOptions
	dir := "./tmp-serial-short"
	opts.DirPath = dir
	db, _ := ComDB.Open(opts)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := db.NewWriteBatch(ComDB.DefaultWriteBatchOptions)
		_ = wb.Put(utils.GetTestKey(i), utils.RandomValue(100)) // 100B短值
		_ = wb.Commit()
	}
}

func Benchmark_SerialLongTxn(b *testing.B) {
	opts := ComDB.DefaultOptions
	dir := "./tmp-serial-long"
	opts.DirPath = dir
	db, _ := ComDB.Open(opts)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := db.NewWriteBatch(ComDB.DefaultWriteBatchOptions)
		// 长事务：批量写入100个键值对
		for j := 0; j < 1000; j++ {
			key := utils.GetTestKey(i*100 + j)
			_ = wb.Put(key, utils.RandomValue(1024)) // 1KB值
		}
		_ = wb.Commit()
	}
}
