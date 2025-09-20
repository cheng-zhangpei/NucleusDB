package benchmark

import (
	NucleusDB "NucleusDB"
	"NucleusDB/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
	"os"
	"testing"
	"time"
)

func Benchmark_Put(b *testing.B) {
	// 为当前测试创建临时目录
	dir, err := os.MkdirTemp("", "bitcask-go-bench-put")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// 初始化数据库
	options := NucleusDB.DefaultOptions
	options.DirPath = dir
	db, err := NucleusDB.Open(options)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// 测试不同value大小的写入性能
	valueSizes := []int{128, 1024, 4096, 8192}
	for _, size := range valueSizes {
		b.Run(fmt.Sprintf("value_size_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := db.Put(utils.GetTestKey(i), utils.RandomValue(size))
				assert.Nil(b, err)
			}
		})
	}
}

func Benchmark_Get(b *testing.B) {
	// 为当前测试创建临时目录
	dir, err := os.MkdirTemp("", "bitcask-go-bench-get")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// 初始化数据库
	options := NucleusDB.DefaultOptions
	options.DirPath = dir
	db, err := NucleusDB.Open(options)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// 预加载不同规模的数据集
	dataSizes := []int{1000, 10000, 100000}
	for _, size := range dataSizes {
		b.Run(fmt.Sprintf("dataset_%d", size), func(b *testing.B) {
			// 准备数据
			for i := 0; i < size; i++ {
				err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
				assert.Nil(b, err)
			}

			rand.Seed(uint64(time.Now().UnixNano()))
			b.ResetTimer()
			b.ReportAllocs()

			// 测试随机读取
			for i := 0; i < b.N; i++ {
				_, err := db.Get(utils.GetTestKey(rand.Intn(size)))
				if err != nil && err != NucleusDB.ErrKeyNotFound {
					b.Fatal(err)
				}
			}
		})
	}
}

func Benchmark_Delete(b *testing.B) {
	dir, err := os.MkdirTemp("", "bitcask-go-bench-delete")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	options := NucleusDB.DefaultOptions
	options.DirPath = dir
	db, err := NucleusDB.Open(options)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	dataSizes := []int{1000, 10000, 100000}
	for _, size := range dataSizes {
		b.Run(fmt.Sprintf("dataset_%d", size), func(b *testing.B) {
			// 准备数据
			keys := make([][]byte, size)
			for i := 0; i < size; i++ {
				keys[i] = utils.GetTestKey(i)
				err := db.Put(keys[i], utils.RandomValue(1024))
				assert.Nil(b, err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			// 测试顺序删除（使用b.N控制循环次数）
			for i := 0; i < b.N; i++ {
				// 使用模运算循环使用keys
				key := keys[i%size]
				err := db.Delete(key)
				assert.Nil(b, err)

				// 重新插入以保证数据存在
				if err == nil {
					err = db.Put(key, utils.RandomValue(1024))
					assert.Nil(b, err)
				}
			}
		})
	}
}
func Benchmark_Mixed(b *testing.B) {
	// 为当前测试创建临时目录
	dir, err := os.MkdirTemp("", "bitcask-go-bench-mixed")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// 初始化数据库
	options := NucleusDB.DefaultOptions
	options.DirPath = dir
	db, err := NucleusDB.Open(options)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// 准备初始数据
	const initialSize = 10000
	for i := 0; i < initialSize; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// 测试不同读写比例
	ratios := []struct {
		name  string
		write float64
	}{
		{"write_heavy", 0.3},
		{"balanced", 0.5},
		{"read_heavy", 0.1},
	}

	for _, ratio := range ratios {
		b.Run(ratio.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// 根据比例决定操作类型
				if rand.Float64() < ratio.write {
					// 写操作
					key := utils.GetTestKey(rand.Intn(initialSize * 2)) // 50%新key
					err := db.Put(key, utils.RandomValue(1024))
					assert.Nil(b, err)
				} else {
					// 读操作
					_, err := db.Get(utils.GetTestKey(rand.Intn(initialSize)))
					if err != nil && err != NucleusDB.ErrKeyNotFound {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
