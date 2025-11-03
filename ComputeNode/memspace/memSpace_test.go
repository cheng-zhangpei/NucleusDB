package memspace

import (
	"fmt"
	_ "log"
	"testing"
)

// MockEmbeddingServerClient 模拟嵌入客户端，用于测试
type MockEmbeddingServerClient struct {
	shouldFail bool
}

func NewMockEmbeddingServerClient(shouldFail bool) *MockEmbeddingServerClient {
	return &MockEmbeddingServerClient{shouldFail: shouldFail}
}

func (m *MockEmbeddingServerClient) Embed(texts []string, dimensions int) ([][]float32, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock embedding failure")
	}

	// 生成模拟向量
	embeddings := make([][]float32, len(texts))
	for i := range texts {
		embedding := make([]float32, dimensions)
		for j := range embedding {
			// 基于文本内容生成确定性模拟向量
			embedding[j] = float32((i+j)%100) / 100.0
		}
		embeddings[i] = embedding
	}
	return embeddings, nil
}

func (m *MockEmbeddingServerClient) EmbedSingle(text string, dimensions int) ([]float32, error) {
	embeddings, err := m.Embed([]string{text}, dimensions)
	if err != nil {
		return nil, err
	}
	return embeddings[0], nil
}

func (m *MockEmbeddingServerClient) HealthCheck() (*HealthResponse, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock health check failure")
	}
	return &HealthResponse{Status: "healthy"}, nil
}

func (m *MockEmbeddingServerClient) GetModels() (*ModelsResponse, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock get models failure")
	}
	return &ModelsResponse{
		CurrentModel:        "text-embedding-v4",
		SupportedDimensions: []int{64, 128, 256, 512, 768, 1024},
	}, nil
}

func (m *MockEmbeddingServerClient) CalculateSimilarity(text1, text2 string, dimensions int) (float64, error) {
	if m.shouldFail {
		return 0, fmt.Errorf("mock similarity calculation failure")
	}
	return 0.75, nil
}

func (m *MockEmbeddingServerClient) BatchTest(texts []string) (map[string]interface{}, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock batch test failure")
	}
	return map[string]interface{}{
		"test_results": "mock_results",
	}, nil
}

// TestMemSpace 测试 MemSpace 功能
func TestMemSpace(t *testing.T) {
	fmt.Println("🧪 Starting MemSpace Tests...")
	fmt.Println("=======================================")

	// 测试1: 创建 MemSpace
	fmt.Println("\n1. Testing MemSpace Creation...")
	testMemSpaceCreation(t)

	// 测试2: 文本预处理
	fmt.Println("\n2. Testing Text Preprocessing...")
	testTextPreprocessing(t)

	// 测试3: 向量生成
	fmt.Println("\n3. Testing Embedding Generation...")
	testEmbeddingGeneration(t)

	// 测试4: 相似度计算
	fmt.Println("\n4. Testing Similarity Calculation...")
	testSimilarityCalculation(t)

	// 测试5: 语义搜索
	fmt.Println("\n5. Testing Semantic Search...")
	testSemanticSearch(t)

	// 测试6: 向量搜索
	fmt.Println("\n6. Testing Vector Search...")
	testVectorSearch(t)

	// 测试7: 批量嵌入
	fmt.Println("\n7. Testing Batch Embedding...")
	testBatchEmbedding(t)

	// 测试8: 统计信息
	fmt.Println("\n8. Testing Statistics...")
	testStatistics(t)

	// 测试9: 错误处理
	fmt.Println("\n9. Testing Error Handling...")
	testErrorHandling(t)

	fmt.Println("\n✅ all MemSpace tests completed!")
}

// testMemSpaceCreation 测试 MemSpace 创建
func testMemSpaceCreation(t *testing.T) {
	// 使用模拟客户端
	mockClient := NewEmbeddingServerClient("http://localhost:5000")

	memSpace := &MemSpace{
		MemSpaceId:            1,
		bindingAgents:         make([]uint64, 0),
		vectorUints:           make([]*VectorRecord, 0),
		spaceType:             Shared,
		spaceStatus:           Pending,
		spaceLimit:            1024 * 1024, // 1MB
		availSpace:            0,
		memSpaceContentType:   ContentMemory,
		embeddingServerClient: mockClient,
	}

	if memSpace.MemSpaceId != 1 {
		t.Errorf("❌ Expected MemSpaceId 1, got %d", memSpace.MemSpaceId)
	}
	if memSpace.spaceType != Shared {
		t.Errorf("❌ Expected spaceType Shared, got %v", memSpace.spaceType)
	}
	if memSpace.memSpaceContentType != ContentMemory {
		t.Errorf("❌ Expected ContentMemory, got %v", memSpace.memSpaceContentType)
	}

	fmt.Println("   ✅ MemSpace creation: PaSS")
}

// testTextPreprocessing 测试文本预处理
func testTextPreprocessing(t *testing.T) {
	memSpace := createTestMemSpace(t, ContentMemory)

	testCases := []struct {
		input    string
		expected string
		desc     string
	}{
		{
			input:    "这是一个测试文本！",
			expected: "这是一个测试文本",
			desc:     "Chinese text with punctuation",
		},
		{
			input:    "Hello, World!  This is a TEST.",
			expected: "hello world this is a test",
			desc:     "English text with punctuation",
		},
		{
			input:    "   Multiple    spaces   and\nnewlines   ",
			expected: "multiple spaces and newlines",
			desc:     "Text with extra spaces",
		},
		{
			input:    "Code: function test() { return true; }",
			expected: "code function test return true",
			desc:     "Code content for content memory",
		},
		{
			input:    "",
			expected: "",
			desc:     "Empty text",
		},
		{
			input:    "Special @#$% characters &*()",
			expected: "special characters",
			desc:     "Text with special characters",
		},
	}

	for _, tc := range testCases {
		result := memSpace.preClean(tc.input)
		if result != tc.expected {
			t.Errorf("❌ Preprocessing failed for '%s': expected '%s', got '%s'",
				tc.desc, tc.expected, result)
		} else {
			fmt.Printf("   ✅ Preprocessing '%s': PASS\n", tc.desc)
		}
	}

	// 测试工具记忆的特殊清理
	toolMemSpace := createTestMemSpace(t, ToolMemory)
	codeText := "function test(a, b) { return a + b; }"
	codeResult := toolMemSpace.preClean(codeText)
	expectedCodeResult := "function test a b return a b"
	if codeResult != expectedCodeResult {
		t.Errorf("❌ Tool memory preprocessing failed: expected '%s', got '%s'",
			expectedCodeResult, codeResult)
	} else {
		fmt.Printf("   ✅ Tool memory preprocessing: PASS ('%s' -> '%s')\n", codeText, codeResult)
	}
}

// testEmbeddingGeneration 测试向量生成
func testEmbeddingGeneration(t *testing.T) {
	memSpace := createTestMemSpace(t, ContentMemory)

	// 测试正常情况
	content := "This is a test content for embedding generation"
	record, err := memSpace.embedding(content)
	if err != nil {
		t.Errorf("❌ Embedding generation failed: %v", err)
		return
	}

	if record == nil {
		t.Errorf("❌ Expected non-nil vector record")
		return
	}

	expectedDimension := 1024 // ContentMemory 推荐维度
	if len(record.data) != expectedDimension {
		t.Errorf("❌ Expected dimension %d, got %d", expectedDimension, len(record.data))
	} else {
		fmt.Printf("   ✅ Embedding generation: PaSS (dimension: %d)\n", len(record.data))
	}

	// 验证向量记录被添加到记忆空间
	if len(memSpace.vectorUints) != 1 {
		t.Errorf("❌ Expected 1 vector unit, got %d", len(memSpace.vectorUints))
	}
}

// testSimilarityCalculation 测试相似度计算
func testSimilarityCalculation(t *testing.T) {
	memSpace := createTestMemSpace(t, ContentMemory)

	// 创建测试向量
	vector1 := &VectorRecord{
		agentId: 1,
		data:    []float32{1.0, 0.0, 0.0}, // 单位向量
	}

	vector2 := &VectorRecord{
		agentId: 1,
		data:    []float32{1.0, 0.0, 0.0}, // 相同向量
	}

	vector3 := &VectorRecord{
		agentId: 1,
		data:    []float32{0.0, 1.0, 0.0}, // 正交向量
	}

	// 测试相同向量
	similarity1 := memSpace.cacSimilarity(*vector1, *vector2)
	if similarity1 != 1.0 {
		t.Errorf("❌ Expected similarity 1.0 for identical vectors, got %.4f", similarity1)
	} else {
		fmt.Printf("   ✅ Identical vectors similarity: PaSS (%.4f)\n", similarity1)
	}

	// 测试正交向量
	similarity2 := memSpace.cacSimilarity(*vector1, *vector3)
	if similarity2 != 0.0 {
		t.Errorf("❌ Expected similarity 0.0 for orthogonal vectors, got %.4f", similarity2)
	} else {
		fmt.Printf("   ✅ Orthogonal vectors similarity: PaSS (%.4f)\n", similarity2)
	}

	// 测试维度不匹配
	vector4 := &VectorRecord{
		agentId: 1,
		data:    []float32{1.0, 0.0}, // 不同维度
	}
	similarity3 := memSpace.cacSimilarity(*vector1, *vector4)
	if similarity3 != 0.0 {
		t.Errorf("❌ Expected similarity 0.0 for dimension mismatch, got %.4f", similarity3)
	} else {
		fmt.Printf("   ✅ Dimension mismatch handling: PaSS\n")
	}
}

// testSemanticSearch 测试语义搜索
func testSemanticSearch(t *testing.T) {
	memSpace := createTestMemSpace(t, ContentMemory)

	// 先添加一些测试数据
	testContents := []string{
		"Machine learning is a subset of artificial intelligence",
		"Deep learning uses neural networks with multiple layers",
		"Natural language processing helps computers understand human language",
		"Computer vision enables machines to interpret visual information",
	}

	for _, content := range testContents {
		_, err := memSpace.embedding(content)
		if err != nil {
			t.Errorf("❌ Failed to add test content: %v", err)
			return
		}
	}

	// 执行语义搜索
	query := "artificial intelligence and neural networks"
	results, err := memSpace.SemanticSearch(query, 3)
	if err != nil {
		t.Errorf("❌ Semantic search failed: %v", err)
		return
	}

	if len(results) == 0 {
		t.Errorf("❌ Expected search results, got none")
	} else {
		fmt.Printf("   ✅ Semantic search: PaSS (found %d results)\n", len(results))
	}

	// 验证返回结果数量不超过请求的 topK
	if len(results) > 3 {
		t.Errorf("❌ Expected max 3 results, got %d", len(results))
	}
}

// testVectorSearch 测试向量搜索
func testVectorSearch(t *testing.T) {
	memSpace := createTestMemSpace(t, ContentMemory)

	// 添加测试向量
	testVectors := [][]float32{
		{1.0, 0.0, 0.0, 0.0},
		{0.9, 0.1, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
		{0.0, 0.0, 1.0, 0.0},
	}

	for i, vector := range testVectors {
		memSpace.vectorUints = append(memSpace.vectorUints, &VectorRecord{
			agentId: uint64(i + 1),
			data:    vector,
		})
	}

	// 搜索相似向量
	queryVector := []float32{1.0, 0.0, 0.0, 0.0}
	results, err := memSpace.SearchByVector(queryVector, 2)
	if err != nil {
		t.Errorf("❌ Vector search failed: %v", err)
		return
	}

	if len(results) != 2 {
		t.Errorf("❌ Expected 2 results, got %d", len(results))
	} else {
		fmt.Printf("   ✅ Vector search: PaSS (found %d results)\n", len(results))
	}

	// 验证结果排序（相似度从高到低）
	if len(results) >= 2 {
		// 第一个结果应该是最相似的
		similarity1 := memSpace.cacSimilarity(*results[0], VectorRecord{data: queryVector})
		similarity2 := memSpace.cacSimilarity(*results[1], VectorRecord{data: queryVector})
		if similarity1 < similarity2 {
			t.Errorf("❌ Results not sorted by similarity: %.4f < %.4f", similarity1, similarity2)
		}
	}
}

// testBatchEmbedding 测试批量嵌入
func testBatchEmbedding(t *testing.T) {
	memSpace := createTestMemSpace(t, ToolMemory)

	contents := []string{
		"function calculateSum(a, b) { return a + b; }",
		"class User { constructor(name) { this.name = name; } }",
		"api.call('/endpoint', { method: 'GET' })",
	}

	records, err := memSpace.BatchEmbedding(contents)
	if err != nil {
		t.Errorf("❌ Batch embedding failed: %v", err)
		return
	}

	if len(records) != len(contents) {
		t.Errorf("❌ Expected %d records, got %d", len(contents), len(records))
	} else {
		fmt.Printf("   ✅ Batch embedding: PaSS (processed %d texts)\n", len(records))
	}

	// 验证所有向量都有正确的维度（ToolMemory 推荐 512 维）
	for i, record := range records {
		if len(record.data) != 512 {
			t.Errorf("❌ Record %d: expected dimension 512, got %d", i, len(record.data))
		}
	}
}

// testStatistics 测试统计信息
func testStatistics(t *testing.T) {
	memSpace := createTestMemSpace(t, BehavioralMemory)

	// 添加一些测试数据
	contents := []string{
		"Decision: use algorithm a for better performance",
		"Best practice: cache frequently accessed data",
		"Optimization: reduce database queries",
	}

	for _, content := range contents {
		_, err := memSpace.embedding(content)
		if err != nil {
			t.Errorf("❌ Failed to add test content: %v", err)
			return
		}
	}

	stats := memSpace.GetVectorStats()

	expectedFields := []string{
		"total_vectors",
		"vector_dimension",
		"space_used",
		"space_limit",
		"space_usage",
	}

	for _, field := range expectedFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("❌ Missing field in stats: %s", field)
		}
	}

	if stats["total_vectors"] != 3 {
		t.Errorf("❌ Expected 3 vectors in stats, got %v", stats["total_vectors"])
	} else {
		fmt.Printf("   ✅ Statistics: PaSS (vectors: %v)\n", stats["total_vectors"])
	}
}

// testErrorHandling 测试错误处理
func testErrorHandling(t *testing.T) {
	fmt.Println("   Testing error scenarios...")

	// 测试1: 空文本嵌入
	memSpace := createTestMemSpace(t, ContentMemory)
	_, err := memSpace.embedding("")
	if err == nil {
		t.Errorf("❌ Expected error for empty text, but got none")
	} else {
		fmt.Printf("   ✅ Empty text error handling: PASS (%v)\n", err)
	}

	// 测试2: 客户端未初始化
	memSpaceNoClient := &MemSpace{
		MemSpaceId:            2,
		vectorUints:           make([]*VectorRecord, 0),
		embeddingServerClient: nil,
	}
	_, err = memSpaceNoClient.embedding("test")
	if err == nil {
		t.Errorf("❌ Expected error for uninitialized client, but got none")
	} else {
		fmt.Printf("   ✅ Uninitialized client error handling: PASS (%v)\n", err)
	}

	// 测试3: 空向量搜索 - 应该返回空结果而不是错误
	results, err := memSpaceNoClient.SearchByVector([]float32{1.0, 0.0}, 5)
	if err != nil {
		t.Errorf("❌ Vector search should not return error for empty client, got: %v", err)
	} else if len(results) != 0 {
		t.Errorf("❌ Expected empty results for empty vector space, got %d", len(results))
	} else {
		fmt.Printf("   ✅ Empty vector search handling: PASS (returned %d results)\n", len(results))
	}

	// 测试4: 空向量空间搜索
	memSpaceEmpty := createTestMemSpace(t, ContentMemory)
	results, err = memSpaceEmpty.SearchByVector([]float32{1.0, 0.0}, 5)
	if err != nil {
		t.Errorf("❌ Vector search should not return error for empty vector space, got: %v", err)
	} else if len(results) != 0 {
		t.Errorf("❌ Expected empty results for empty vector space, got %d", len(results))
	} else {
		fmt.Printf("   ✅ Empty vector space search handling: PASS (returned %d results)\n", len(results))
	}
}

// createTestMemSpace 创建测试用的 MemSpace
func createTestMemSpace(t *testing.T, contentType MemSpaceContentType) *MemSpace {
	mockClient := NewEmbeddingServerClient("http://localhost:5000")

	return &MemSpace{
		MemSpaceId:            1,
		bindingAgents:         make([]uint64, 0),
		vectorUints:           make([]*VectorRecord, 0),
		spaceType:             Shared,
		spaceStatus:           Pending,
		spaceLimit:            1024 * 1024,
		availSpace:            0,
		memSpaceContentType:   contentType,
		embeddingServerClient: mockClient,
	}
}

//func TestMemSpaceVectorInformal(t *testing.T) {
//	space := createTestMemSpace(t, ContentMemory)
//	// 自己测测空间如何
//	space.SearchByVector() {
//}
