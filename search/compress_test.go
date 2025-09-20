package search

import (
	"NucleusDB"
	"github.com/stretchr/testify/assert"
	"testing"
)

// todo 在测试的时候总觉得速度太慢，后续还要优化，估计有啥奇怪的地方瓶颈了，但是内存的确是大问题，反复的抽象带来了很多的内存消耗
func TestCompressor_Compress(t *testing.T) {
	opts := NucleusDB.DefaultOptions
	compressOpts := NucleusDB.DefaultCompressOptions

	agentId := "testAgentId1"
	opts.DirPath = "/tmp/bitcask_memory_test"

	ms, err := NewMemoryStructure(opts, agentId, 10)
	assert.Nil(t, err)
	assert.NotNil(t, ms)
	// 往其中插入10条数据
	testData := [][]string{
		{"我喜欢用 Go 语言编程，因为它简洁高效，其独特的设计使得开发者能够专注于编写逻辑清晰的代码。通过使用 goroutine 和 channel，开发者可以更高效地管理并发任务，这在许多高性能场景中极为重要。"},
		{"Go 语言非常适合并发编程，尤其是在处理高并发场景时，其简洁的语法、强大的库支持和内置的并发模型使得它成为开发高性能服务器和分布式系统的理想选择。"},
		{"我享受用 Go 语言写代码，它的语法非常直观，能够让开发者快速上手并实现复杂功能。无论是 Web 服务还是命令行工具，Go 语言都能够很好地胜任，同时保持代码的可读性和可维护性。"},
		{"Python 也是一种流行的编程语言，特别适合数据科学和机器学习。它提供了丰富的库和工具支持，例如 NumPy、Pandas 和 TensorFlow，能够帮助开发者快速构建数据处理和模型训练的工作流。"},
		{"Java 在企业级应用中非常流行，尤其是在大型系统中，它通过强大的生态系统和框架（如 Spring 和 Hibernate）支持开发人员快速构建复杂的企业解决方案。"},
		{"C++ 是一种高性能的编程语言，常用于游戏开发和系统编程。其强大的底层控制能力和广泛的库支持使得开发者能够高效地开发性能要求极高的应用程序，同时确保稳定性和可扩展性。"},
		{"JavaScript 是前端开发的首选语言，几乎所有的现代网站都使用它。无论是简单的交互效果还是复杂的单页应用，JavaScript 的强大生态系统都提供了高效开发的可能性。"},
		{"Rust 是一种系统编程语言，注重安全性和并发性。其独特的所有权模型可以在保证高性能的同时有效避免常见的内存问题，为开发者带来了前所未有的开发体验。"},
		{"Kotlin 是一种现代编程语言，逐渐成为 Android 开发的主流选择。它的简洁语法和强大的功能使得开发者能够更高效地编写高质量代码，同时兼容现有的 Java 项目。"},
		{"Swift 是苹果公司开发的编程语言，主要用于 iOS 和 macOS 应用开发。它的现代化设计、简洁语法和安全特性使得开发者能够快速构建出色的用户体验，同时提高代码的可靠性。"},
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
		"Go 语言非常适合高并发场景，无论是开发微服务架构还是实时通信系统，其内置的并发支持都让开发者倍感轻松。",
		"通过 goroutine 和 channel，Go 语言能够在高并发编程中展现出极高的效率。这种灵活的并发模型让它成为构建实时系统的绝佳选择。",
		"Go 语言的并发机制让开发者能够轻松管理数以千计的并发任务，从而有效提升系统的吞吐量。",
		"高并发场景下，Go 语言的 goroutine 比传统线程更轻量级，结合高效的调度器，能够显著提升系统性能。",
		"对于高并发编程来说，Go 的内置工具和简洁语法让开发流程更加高效，从而降低了复杂度和开发成本。",

		// 第二组匹配数据（匹配第 4 条数据）
		"Python 提供了大量开箱即用的工具，使其成为数据分析、人工智能和机器学习领域的理想选择。",
		"借助于强大的生态系统，Python 可以快速完成从数据清洗到模型部署的全流程，特别适合需要高生产力的项目。",
		"在处理数据科学任务时，Python 的灵活性和强大的库支持（如 Matplotlib 和 Seaborn）让它成为数据可视化的首选。",
		"对于机器学习项目来说，Python 的 TensorFlow 和 PyTorch 框架提供了丰富的功能，简化了深度学习模型的开发和训练。",

		// 第三组匹配数据（匹配第 6 条数据）
		"C++ 是开发需要极致性能的应用程序的首选，无论是图形处理、物理模拟还是游戏引擎开发，其表现都十分出色。",
		"游戏引擎 Unreal Engine 和 Unity 都对 C++ 提供了全面支持，进一步巩固了它在高性能开发中的地位。",
		"得益于其灵活的内存管理和强大的标准模板库（STL），C++ 在高负载计算领域表现优异，广泛应用于科学计算和实时仿真。",
		"C++ 提供了对硬件的高度控制能力，尤其是在开发底层系统和驱动程序时，其性能和稳定性无可替代。",
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
	compressOptions := NucleusDB.DefaultCompressOptions
	modelOptions := NucleusDB.DefaultCompressModelOptions
	compressor, err := NewCompressor(compressOptions, agentId, ms)
	assert.Nil(t, err)
	assert.NotNil(t, compressor)
	compress, err := compressor.Compress(agentId, modelOptions.Endpoint)
	assert.Nil(t, err)
	assert.NotNil(t, compress)
}
