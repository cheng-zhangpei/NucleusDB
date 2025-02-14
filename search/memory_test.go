package search

import (
	"ComDB"
	"fmt"
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
	value1 := []byte("value1")
	value2 := []byte("value2")

	// 测试 MMSet
	err = ms.MMSet(value1, "agent1")
	assert.NoError(t, err, "MMSet failed")

	// 等待一段时间，确保时间戳不同
	time.Sleep(1 * time.Millisecond)

	// 再次调用 MMSet
	err = ms.MMSet(value2, "agent1")
	assert.NoError(t, err, "MMSet failed")

	// 测试 MMGet
	result, err := ms.MMGet("agent1")
	assert.NoError(t, err, "MMGet failed")

	// 验证结果
	expected := "timeNear:0,value:value2\ntimeNear:1,value:value1\n"
	assert.Equal(t, expected, result, "Unexpected result")

	// 验证 memorySize
	meta, err := ms.FindMetaData([]byte("agent1"))
	assert.NoError(t, err, "Failed to find meta data")
	assert.Equal(t, int64(2), meta.GetMemorySize(), "Unexpected memory size")

	// 验证时间戳顺序
	timestamps := meta.GetAllMemory()
	assert.Equal(t, 2, len(timestamps), "Unexpected number of timestamps")
	assert.True(t, timestamps[0] > timestamps[1], "Timestamps are not in descending order")
}

// 测试 MemoryStructure 的功能
func TestMemoryStructureWithChinese(t *testing.T) {
	// 初始化 MemoryStructure
	opts := ComDB.DefaultOptions
	optsCompress := ComDB.DefaultCompressOptions
	opts.DirPath = "/tmp/bitcask_memory_test"
	agentId := "agent1"
	totalSize := int64(10)
	ms, err := NewMemoryStructure(opts, agentId, totalSize)
	if err != nil {
		t.Fatalf("Failed to create MemoryStructure: %v", err)
	}

	// 插入多条中文数据
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "我喜欢用 Go 语言编程，因为它简洁高效。"},
		{"key2", "Go 语言非常适合并发编程，尤其是在处理高并发场景时。"},
		{"key3", "我享受用 Go 语言写代码，它的语法非常直观。"},
		{"key4", "Python 也是一种流行的编程语言，特别适合数据科学和机器学习。"},
		{"key5", "Java 在企业级应用中非常流行，尤其是在大型系统中。"},
		{"key6", "C++ 是一种高性能的编程语言，常用于游戏开发和系统编程。"},
		{"key7", "JavaScript 是前端开发的首选语言，几乎所有的现代网站都使用它。"},
		{"key8", "Rust 是一种系统编程语言，注重安全性和并发性。"},
		{"key9", "Kotlin 是一种现代编程语言，逐渐成为 Android 开发的主流选择。"},
		{"key10", "Swift 是苹果公司开发的编程语言，主要用于 iOS 和 macOS 应用开发。"},
	}

	for _, data := range testData {
		err := ms.MMSet([]byte(data.value), agentId)
		if err != nil {
			t.Fatalf("Failed to set memory: %v", err)
		}
	}

	// 测试 MMGet：获取所有记忆
	allMemory, err := ms.MMGet(agentId)
	if err != nil {
		t.Fatalf("Failed to get memory: %v", err)
	}
	fmt.Printf("All Memory:")
	fmt.Printf(allMemory)

	// 测试 matchSearch：匹配搜索
	testCases := []struct {
		searchItem string
		expected   string
	}{
		{"Go 语言", "我喜欢用 Go 语言编程，因为它简洁高效。, Go 语言非常适合并发编程，尤其是在处理高并发场景时。, 我享受用 Go 语言写代码，它的语法非常直观。"},
		{"Python", "Python 也是一种流行的编程语言，特别适合数据科学和机器学习。"},
		{"Java", "Java 在企业级应用中非常流行，尤其是在大型系统中。"},
		{"C++", "C++ 是一种高性能的编程语言，常用于游戏开发和系统编程。"},
		{"JavaScript", "JavaScript 是前端开发的首选语言，几乎所有的现代网站都使用它。"},
		{"Rust", "Rust 是一种系统编程语言，注重安全性和并发性。"},
		{"Kotlin", "Kotlin 是一种现代编程语言，逐渐成为 Android 开发的主流选择。"},
		{"Swift", "Swift 是苹果公司开发的编程语言，主要用于 iOS 和 macOS 应用开发。"},
		{"Ruby", ""}, // 预期无匹配
	}

	for _, tc := range testCases {
		result, err := ms.MatchSearch(tc.searchItem, agentId, optsCompress)
		if err != nil {
			t.Fatalf("Failed to match search: %v", err)
		}
		fmt.Printf("Search Item: %s\n", tc.searchItem)
		fmt.Printf("Expected: %s\n", tc.expected)
		fmt.Printf("Actual: %s\n", result)
	}
}
