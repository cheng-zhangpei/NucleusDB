package ComDB

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 测试MVCC的基本功能

func Test_base_mvcc(t *testing.T) {
	opts := DefaultOptions
	dir := "./tmp/"
	opts.DirPath = dir
	//defer os.RemoveAll(dir) // 测试完成后清理

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	update := NewUpdate(5, db)

	// 测试1: 基本Put/Get操作
	t.Run("basic put/get", func(t *testing.T) {
		results, err := update.Update(func(txn *Txn) error {
			if err := txn.Put([]byte("key1"), []byte("value1")); err != nil {
				return err
			}
			if err := txn.Put([]byte("key2"), []byte("value2")); err != nil {
				return err
			}
			if err := txn.Delete([]byte("key1")); err != nil {
				return err
			}
			return txn.Get([]byte("key1"))
		})
		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
		if len(results) != 1 || string(results[0]) != "value1" {
			t.Errorf("Unexpected get result: %v", results)
		}
	})

	// 测试2: 重复写入检测
	t.Run("duplicate write", func(t *testing.T) {
		result, err := update.Update(func(txn *Txn) error {
			if err := txn.Put([]byte("key1"), []byte("new_value")); err != nil {
				return err
			}
			return txn.Get([]byte("key1"))
		})
		if result != nil && err == nil {
			log.Println("case 2: " + result[0])
		}
		tempo, _ := db.Get([]byte("key1"))
		log.Println(tempo)
		if err != nil {
			t.Fatalf("Duplicate write failed: %v", err)
		}

		// 验证旧版本是否被保留在pendingRepeatWrites
		if len(update.Tracker.waterToTxn) == 0 {
			t.Error("Expected pendingRepeatWrites to contain old value")
		}
	})

	// 测试3: 删除操作
	t.Run("delete", func(t *testing.T) {
		_, err := update.Update(func(txn *Txn) error {
			return txn.Delete([]byte("key1"))
		})
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// 验证删除后读取
		_, err = update.Update(func(txn *Txn) error {
			return txn.Get([]byte("key1"))
		})
		if err == nil {
			t.Error("Expected error when reading deleted key")
		}
	})
}

func Test_n_txn_con(t *testing.T) {
	opts := DefaultOptions
	dir := "./tmp/"
	opts.DirPath = dir
	//defer os.RemoveAll(dir) // 测试完成后清理

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	update := NewUpdate(5, db)
	t.Log("数据库和更新器初始化完成")

	// 初始化测试数据
	_, err = update.Update(func(txn *Txn) error {
		t.Log("初始化测试数据: 设置 balance = 100")
		return txn.Put([]byte("balance"), []byte("100"))
	})
	if err != nil {
		t.Fatal(err)
	}

	// 并发测试参数
	concurrency := 10
	var wg sync.WaitGroup
	wg.Add(concurrency)

	// 并发转账测试
	t.Run("concurrent transfer", func(t *testing.T) {
		t.Logf("开始并发转账测试，并发数: %d", concurrency)

		for i := 0; i < concurrency; i++ {
			go func(id int) {
				defer wg.Done()
				t.Logf("协程 %d 开始执行转账", id)

				_, err := update.Update(func(txn *Txn) error {
					// 读取当前余额
					if err := txn.Get([]byte("balance")); err != nil {
						t.Logf("协程 %d 读取余额失败: %v", id, err)
						return err
					}

					t.Logf("协程 %d 准备更新余额为 110", id)
					// 模拟转账操作
					return txn.Put([]byte("balance"), []byte("110"))
				})

				if err != nil {
					t.Logf("协程 %d 转账失败 (预期中可能有部分失败): %v", id, err)
				} else {
					t.Logf("协程 %d 转账成功", id)
				}
			}(i)
		}
		wg.Wait()
		t.Log("所有转账协程执行完毕")

		// 验证最终余额
		results, err := update.Update(func(txn *Txn) error {
			return txn.Get([]byte("balance"))
		})
		if err != nil {
			t.Fatal("验证最终余额时出错:", err)
		}
		t.Logf("最终余额验证结果: %s", results[0])
		if string(results[0]) != "110" {
			t.Errorf("意外的最终余额: %s (预期: 110)", results[0])
		}
	})

	// 冲突事务测试
	t.Run("conflict detection", func(t *testing.T) {
		t.Log("开始显式冲突检测测试")

		// 启动两个会冲突的事务
		var conflictCounter int32
		wg.Add(2)

		go func() {
			defer wg.Done()
			t.Log("冲突事务1 开始执行")

			_, err := update.Update(func(txn *Txn) error {
				t.Log("冲突事务1 休眠50ms确保重叠")
				time.Sleep(50 * time.Millisecond)

				t.Log("冲突事务1 尝试写入 conflict_key = value1")
				return txn.Put([]byte("conflict_key"), []byte("value1"))
			})

			if err != nil {
				t.Logf("冲突事务1 失败 (预期行为): %v", err)
				atomic.AddInt32(&conflictCounter, 1)
			} else {
				t.Log("冲突事务1 成功完成")
			}
		}()

		go func() {
			defer wg.Done()
			t.Log("冲突事务2 开始执行")

			_, err := update.Update(func(txn *Txn) error {
				t.Log("冲突事务2 休眠50ms确保重叠")
				time.Sleep(50 * time.Millisecond)

				t.Log("冲突事务2 尝试写入 conflict_key = value2")
				return txn.Put([]byte("conflict_key"), []byte("value2"))
			})

			if err != nil {
				t.Logf("冲突事务2 失败 (预期行为): %v", err)
				atomic.AddInt32(&conflictCounter, 1)
			} else {
				t.Log("冲突事务2 成功完成")
			}
		}()

		wg.Wait()
		t.Logf("冲突检测完成，失败事务数: %d", conflictCounter)

		if conflictCounter < 1 {
			t.Error("预期至少一个事务因冲突失败，但实际没有检测到冲突")
		} else {
			t.Log("冲突检测结果符合预期")
		}

		// 打印最终写入的值
		results, err := update.Update(func(txn *Txn) error {
			return txn.Get([]byte("conflict_key"))
		})
		if err != nil {
			t.Log("读取冲突键失败:", err)
		} else {
			t.Logf("冲突键最终值: %s", results[0])
		}
	})
}

func Test_watermark_gc(t *testing.T) {
	opts := DefaultOptions
	dir := "./tmp/"
	opts.DirPath = dir
	defer os.RemoveAll(dir)

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 创建小容量watermark
	update := NewUpdate(3, db)
	txnTimestamps := make([]uint64, 5) // 记录事务提交时间

	// 提交5个事务（超过watermark容量）
	for i := 0; i < 5; i++ {
		_, err := update.Update(func(txn *Txn) error {
			return txn.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// 验证watermark大小
	currentSize := update.Watermark.timesHeap.Size()
	if currentSize > 3 {
		t.Fatalf("Watermark size %d 超过maxSize 3", currentSize)
	}

	// 同步执行GC并等待完成
	done := make(chan struct{})
	go func() {
		update.GC()
		close(done)
	}()
	<-done // 阻塞等待GC完成

	// 验证被清理的事务
	t.Log("===== GC后状态验证 =====")
	for i, ts := range txnTimestamps {
		_, exists := update.Tracker.waterToTxn[ts]
		if i < 2 { // 前2个事务应该被清理
			if exists {
				t.Errorf("事务 %d (时间戳: %d) 应被GC清理但仍存在", i, ts)
			}
		} else { // 后3个事务应保留
			if !exists {
				t.Errorf("事务 %d (时间戳: %d) 应保留但被错误清理", i, ts)
			}
		}
	}

	// 验证数据可访问性
	t.Log("===== 数据可访问性验证 =====")
	for i := 0; i < 5; i++ {
		val, err := db.Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Errorf("读取key%d失败: %v", i, err)
		} else {
			t.Logf("key%d = %s", i, val)
		}
	}
}
