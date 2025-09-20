package benchmark

import (
	"ComDB" // 导入你的ComDB包
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
)

// Benchmark_ConcurrentTxn 测试并发事务吞吐量
func Benchmark_ConcurrentTxn(b *testing.B) {
	opts := ComDB.DefaultOptions
	opts.DirPath = "./tmp-bench" // 测试专用目录
	db, err := ComDB.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	update := ComDB.NewUpdate(1000, db) // 大容量watermark
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}

	// 初始化测试数据
	_, err = update.Update(func(txn *ComDB.Txn) error {
		for _, key := range keys {
			if err := txn.Put(key, []byte("init_value")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = update.Update(func(txn *ComDB.Txn) error {
				// 随机读写操作
				key := keys[rand.Intn(len(keys))]
				_ = txn.Get(key)                     // 读
				return txn.Put(key, []byte("value")) // 写
			})
		}
	})
}

func Benchmark_MVCCConflict(b *testing.B) {
	db, _ := ComDB.Open(ComDB.DefaultOptions)
	defer db.Close()

	update := ComDB.NewUpdate(1000, db)
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}

	// 初始化测试数据
	_, _ = update.Update(func(txn *ComDB.Txn) error {
		for _, key := range keys {
			_ = txn.Put(key, []byte("init_value"))
		}
		return nil
	})

	var conflictCount uint64
	b.ResetTimer()

	// 模拟读写混合事务
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := update.Update(func(txn *ComDB.Txn) error {
				// 阶段1：写入多个key（制造修改点）
				for i := 0; i < 3; i++ {
					key := keys[rand.Intn(len(keys))]
					if err := txn.Put(key, []byte("new_value")); err != nil {
						return err
					}
				}
				// 阶段3：读取检查（冲突触发点）
				for i := 0; i < 2; i++ {
					key := keys[rand.Intn(len(keys))]
					if err := txn.Get(key); err != nil {
						return err
					}
				}
				return nil
			})

			if err != nil {
				atomic.AddUint64(&conflictCount, 1)
			}
		}
	})

	b.ReportMetric(
		float64(conflictCount)/float64(b.N)*100,
		"%_conflicts",
	)
}

// Benchmark_LongRunningTxn 测试长事务稳定性
func Benchmark_LongRunningTxn(b *testing.B) {
	db, err := ComDB.Open(ComDB.DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	update := ComDB.NewUpdate(1000, db)
	keys := generateKeys(1000) // 生成1000个测试key

	// 初始化数据
	_, err = update.Update(func(txn *ComDB.Txn) error {
		for _, key := range keys {
			if err := txn.Put(key, []byte("init")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 模拟长事务：读取大量数据后修改少量key
		_, _ = update.Update(func(txn *ComDB.Txn) error {
			for j := 0; j < 100; j++ { // 读100个key
				_ = txn.Get(keys[rand.Intn(len(keys))])
			}
			return txn.Put(keys[i%len(keys)], []byte("new")) // 写1个key
		})
	}
}

// Benchmark_WatermarkGC 测试水位线GC性能
func Benchmark_WatermarkGC(b *testing.B) {
	db, err := ComDB.Open(ComDB.DefaultOptions)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	update := ComDB.NewUpdate(100, db) // 小容量watermark加压GC
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 提交事务触发GC
		_, _ = update.Update(func(txn *ComDB.Txn) error {
			return txn.Put([]byte("key"), []byte("value"))
		})
		if i%10 == 0 { // 每10次强制GC
			update.GC()
		}
	}
}

// 辅助函数：生成测试用key
func generateKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
	}
	return keys
}
