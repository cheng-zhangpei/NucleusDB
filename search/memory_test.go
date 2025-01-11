package search

import (
	"ComDB"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// 测试 MMSet 和 MMGet 方法
func TestMemoryStructure(t *testing.T) {
	// 初始化数据库选项
	opts := ComDB.DefaultOptions
	opts.DirPath = "/tmp/bitcask_memory_test" // 测试用的临时目录

	// 创建 MemoryStructure
	ms, err := NewMemoryStructure(opts, "agent1", 10)
	assert.NoError(t, err, "Failed to create MemoryStructure")

	// 定义测试数据
	key := []byte("testKey")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// 测试 MMSet
	err = ms.MMSet(key, value1, "agent1")
	assert.NoError(t, err, "MMSet failed")

	// 等待一段时间，确保时间戳不同
	time.Sleep(1 * time.Millisecond)

	// 再次调用 MMSet
	err = ms.MMSet(key, value2, "agent1")
	assert.NoError(t, err, "MMSet failed")

	// 测试 MMGet
	result, err := ms.MMGet(key, "agent1")
	assert.NoError(t, err, "MMGet failed")

	// 验证结果
	expected := "timeNear:0,value:value2\ntimeNear:1,value:value1\n"
	assert.Equal(t, expected, result, "Unexpected result")

	// 验证 memorySize
	meta, err := ms.findMetaData(key)
	assert.NoError(t, err, "Failed to find meta data")
	assert.Equal(t, int64(2), meta.GetMemorySize(), "Unexpected memory size")

	// 验证时间戳顺序
	timestamps := meta.GetAllMemory()
	assert.Equal(t, 2, len(timestamps), "Unexpected number of timestamps")
	assert.True(t, timestamps[0] > timestamps[1], "Timestamps are not in descending order")
}
