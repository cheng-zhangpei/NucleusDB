package search

import (
	"ComDB"
	"github.com/stretchr/testify/assert"
	"testing"
)

// todo 在测试的时候总觉得速度太慢，后续还要优化，估计有啥奇怪的地方瓶颈了，但是内存的确是大问题，反复的抽象带来了很多的内存消耗
func TestCompressor_Compress(t *testing.T) {
	opts := ComDB.DefaultOptions
	compressOpts := ComDB.DefaultCompressOptions

	agentId := "testAgentId1"
	opts.DirPath = "/tmp/bitcask_memory_test"

	ms, err := NewMemoryStructure(opts, agentId, 10)
	assert.Nil(t, err)
	assert.NotNil(t, ms)
	// 往其中插入10条数据
	testData := [][]string{
		{"我喜欢用 Go 语言编程，因为它简洁高效。"},
		{"Go 语言非常适合并发编程，尤其是在处理高并发场景时。"},
		{"我享受用 Go 语言写代码，它的语法非常直观。"},
		{"Python 也是一种流行的编程语言，特别适合数据科学和机器学习。"},
		{"Java 在企业级应用中非常流行，尤其是在大型系统中。"},
		{"C++ 是一种高性能的编程语言，常用于游戏开发和系统编程。"},
		{"JavaScript 是前端开发的首选语言，几乎所有的现代网站都使用它。"},
		{"Rust 是一种系统编程语言，注重安全性和并发性。"},
		{"Kotlin 是一种现代编程语言，逐渐成为 Android 开发的主流选择。"},
		{"Swift 是苹果公司开发的编程语言，主要用于 iOS 和 macOS 应用开发。"},
	}

	// 插入数据
	for _, data := range testData {
		err := ms.MMSet([]byte(data[0]), agentId)
		if err != nil {
			t.Fatalf("Failed to set memory: %v", err)
		}
	}
	// 让匹配度达到阈值并触发压缩操作
	matchData := []string{
		// 第一组匹配数据（匹配第 2 条数据）
		"Go 语言非常适合并发编程，尤其是在处理高并发场景时。",
		"Go 语言特别适合高并发场景，尤其是在处理并发任务时。",
		"Go 语言的并发模型非常强大，特别适合高并发场景。",
		"Go 语言的并发模型非常强大，特别适合高并发场景。",
		"Go 语言的并发模型非常强大，特别适合高并发场景。",

		// 第二组匹配数据（匹配第 4 条数据）
		"Python 也是一种流行的编程语言，特别适合数据科学和机器学习。",
		"Python 在数据科学和机器学习领域非常流行，是一种强大的编程语言。",
		"Python 是数据科学和机器学习的首选语言，应用非常广泛。",

		// 第三组匹配数据（匹配第 6 条数据）
		"C++ 是一种高性能的编程语言，常用于游戏开发和系统编程。",
		"C++ 在游戏开发和系统编程中非常流行，因为它具有高性能。",
		"C++ 是一种高性能语言，特别适合游戏开发和系统编程。",

		// 第四组匹配数据（匹配第 7 条数据）
		"JavaScript 是前端开发的首选语言，几乎所有的现代网站都使用它。",
		"JavaScript 在前端开发中占据主导地位，几乎所有的网页都依赖它。",
		"JavaScript 是构建现代网页的核心技术，应用非常广泛。",

		// 第五组匹配数据（匹配第 8 条数据）
		"Rust 是一种系统编程语言，注重安全性和并发性。",
		"Rust 在系统编程中非常流行，因为它提供了内存安全和并发支持。",
		"Rust 是一种现代系统编程语言，特别适合需要高性能和安全性的场景。",

		// 第六组匹配数据（匹配第 9 条数据）
		"Kotlin 是一种现代编程语言，逐渐成为 Android 开发的主流选择。",
		"Kotlin 在 Android 开发中越来越受欢迎，逐渐取代 Java 成为主流。",
		"Kotlin 是 Android 开发的未来，提供了更简洁和安全的语法。",

		// 第七组匹配数据（匹配第 10 条数据）
		"Swift 是苹果公司开发的编程语言，主要用于 iOS 和 macOS 应用开发。",
		"Swift 是开发 iOS 和 macOS 应用的首选语言，由苹果公司维护。",
		"Swift 是一种现代编程语言，专为苹果生态系统设计。",

		// 第八组匹配数据（匹配第 1 条数据）
		"我喜欢用 Go 语言编程，因为它简洁高效。",
		"Go 语言的简洁和高效让我非常喜欢用它编程。",
		"Go 语言的设计理念让我觉得编程变得更加简单和高效。",

		// 第九组匹配数据（匹配第 3 条数据）
		"我享受用 Go 语言写代码，它的语法非常直观。",
		"Go 语言的直观语法让我在写代码时感到非常享受。",
		"用 Go 语言写代码是一种享受，因为它的语法设计非常清晰。",

		// 第十组匹配数据（匹配第 5 条数据）
		"Java 在企业级应用中非常流行，尤其是在大型系统中。",
		"Java 是开发企业级应用的首选语言，特别适合大型系统。",
		"Java 在企业级开发中占据重要地位，尤其是在构建大型系统时。",
	}
	//todo: 需要检查 1.相似度index位置。2。相似度的数量是否能够对得上.
	for _, data := range matchData {
		// 一定要记得区分用户的Match操作只是为了获取记忆从而再次生成数据而不是需要对记忆空间进行类似插入的工作
		search, err := ms.MatchSearch(data, agentId, compressOpts)
		assert.Nil(t, err)
		assert.NotNil(t, search)
		t.Log(search)
	}
	mm, err := ms.FindMetaData([]byte(agentId))
	if mm == nil {
		t.Fatalf("Failed to find meta data: %v", err)
	}

	// 执行压缩 -> 这里需要看一下压缩器是否可以将匹配区的数据正常的解码
	compressOptions := ComDB.DefaultCompressOptions
	modelOptions := ComDB.DefaultCompressModelOptions
	compressor, err := NewCompressor(compressOptions, agentId, ms)
	assert.Nil(t, err)
	assert.NotNil(t, compressor)
	compress, err := compressor.Compress(agentId, modelOptions.Endpoint)
	assert.Nil(t, err)
	assert.NotNil(t, compress)
}
