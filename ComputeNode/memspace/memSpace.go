package memspace

import (
	"fmt"
	"log"
	"strings"
)

// MemSpaceType the type pf the memspace
type MemSpaceType int

const (
	Private MemSpaceType = iota
	Shared
)

type MemSpaceContentType int

const (
	ToolMemory       MemSpaceContentType = iota // 工具使用记忆（函数调用、API使用记录）
	ContentMemory                               // 内容记忆（对话、文档、知识）
	BehavioralMemory                            // 行为模式记忆（决策逻辑、最佳实践）
	EpisodicMemory                              // 情景记忆（具体事件、会话记录）)
)

// MemSpaceStatus the status of the memsapce
type MemSpaceStatus int

const (
	Pending MemSpaceStatus = iota
	Binding
	Corrupt
	Writing // there have another agent update the space
)

type MemSpace struct {
	// MemSpaceId can not repeat in a system
	MemSpaceId uint64
	// allow multi-agent binding
	BindingAgents []uint64
	// persistent memory uint layout
	memUints []*MemUint
	// content of temp conversation
	TempMemUnits []*TempMemUnit
	// vector datatype record
	vectorUints []*VectorRecord
	// the type of the memSpace
	spaceType MemSpaceType
	// status
	spaceStatus MemSpaceStatus
	spaceLimit  uint64
	availSpace  uint64
	// Memory Space description
	description string

	name                string
	memSpaceContentType MemSpaceContentType
	//	Certain metrics such as similarity used in vector
	//	computations, along with metadata within the memory space.
	//computeMetric *compute.QualityMetrics
	embeddingServerClient *EmbeddingServerClient
}

func NewMemSpace(id uint64, spaceType MemSpaceType,
	spaceLimit uint64, memSpaceContentType MemSpaceContentType,
	embeddingServerAddr string) *MemSpace {
	embeddingClient := NewEmbeddingServerClient(embeddingServerAddr)
	return &MemSpace{
		MemSpaceId:          id,
		BindingAgents:       make([]uint64, 0),
		memUints:            make([]*MemUint, 0),
		TempMemUnits:        make([]*TempMemUnit, 0),
		vectorUints:         make([]*VectorRecord, 0),
		spaceType:           spaceType,
		spaceStatus:         Pending,
		spaceLimit:          spaceLimit,
		availSpace:          0,
		memSpaceContentType: memSpaceContentType,
		//computeMetric: &compute.QualityMetrics{},
		embeddingServerClient: embeddingClient,
		// todo 这里需要加一个锁，等到多智能体协同的时候还是需要注意的
	}
}

// ---------------------------Persist memory operation: I want this part focus on memory record operations----------------------------

func (ms *MemSpace) PersistMemoryUint(key string, data []byte) error {

	return nil
}
func (ms *MemSpace) GetPersistMemoryUint(key string) ([]byte, error) {
	return nil, nil
}
func (ms *MemSpace) UpdatePersistMemory(key string, data []byte) error {
	return nil
}
func (ms *MemSpace) DeletePersistMemory(key string) error {
	return nil
}
func (ms *MemSpace) ListPersistMemories() []string {
	return nil
}

// ---------------------------agent operation----------------------------

func (ms *MemSpace) BindAgent(agentID uint64) error {
	return nil
}
func (ms *MemSpace) UnbindAgent(agentID uint64) error {
	return nil
}
func (ms *MemSpace) GetBoundAgents() []uint64 {
	return ms.BindingAgents
}
func (ms *MemSpace) IsAgentBound(agentID uint64) bool {
	return false
}

// canBinding space can binding?
func (ms *MemSpace) canBinding() bool {
	return false
}

// SearchByVector 使用查询向量搜索相似记忆
func (ms *MemSpace) SearchByVector(queryVector []float32, topK int) ([]*VectorRecord, error) {
	// 如果客户端未初始化，返回空结果而不是错误
	if ms.embeddingServerClient == nil {
		log.Printf("Warning: embedding server client not initialized, returning empty results")
		return []*VectorRecord{}, nil
	}

	if len(ms.vectorUints) == 0 {
		return []*VectorRecord{}, nil
	}

	// 计算相似度并排序
	type SimilarityResult struct {
		Record     *VectorRecord
		Similarity float32
	}

	var results []SimilarityResult

	for _, record := range ms.vectorUints {
		similarity := ms.cacSimilarity(*record, VectorRecord{data: queryVector})
		results = append(results, SimilarityResult{
			Record:     record,
			Similarity: similarity,
		})
	}

	// 按相似度降序排序
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Similarity > results[i].Similarity {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	// 返回前 topK 个结果
	var topRecords []*VectorRecord
	for i := 0; i < topK && i < len(results); i++ {
		topRecords = append(topRecords, results[i].Record)
	}

	log.Printf("Vector search completed: found %d results, returning top %d",
		len(results), len(topRecords))
	return topRecords, nil
}

// SemanticSearch 语义搜索 - 将查询文本转换为向量后搜索
func (ms *MemSpace) SemanticSearch(queryText string, topK int) ([]*VectorRecord, error) {
	if ms.embeddingServerClient == nil {
		return nil, fmt.Errorf("embedding server client not initialized")
	}

	// 预处理查询文本
	cleanedQuery := ms.preClean(queryText)
	if cleanedQuery == "" {
		return nil, fmt.Errorf("query text is empty after cleaning")
	}

	// 生成查询向量
	queryVector, err := ms.embeddingServerClient.EmbedSingle(cleanedQuery, 1024)
	if err != nil {
		return nil, fmt.Errorf("failed to generate query embedding: %v", err)
	}

	log.Printf("Generated query vector with dimension: %d", len(queryVector))

	// 使用向量搜索
	return ms.SearchByVector(queryVector, topK)
}

// embedding 将字符串内容转换为向量并添加到记忆空间
func (ms *MemSpace) embedding(content string) (*VectorRecord, error) {
	if ms.embeddingServerClient == nil {
		return nil, fmt.Errorf("embedding server client not initialized")
	}

	// 预处理文本
	cleanedContent := ms.preClean(content)
	if cleanedContent == "" {
		return nil, fmt.Errorf("content is empty after cleaning")
	}

	// 根据记忆空间类型选择维度
	dimensions := ms.getRecommendedDimensions()

	// 生成嵌入向量
	vector, err := ms.embeddingServerClient.EmbedSingle(cleanedContent, dimensions)
	if err != nil {
		return nil, fmt.Errorf("embedding generation failed: %v", err)
	}

	// 创建向量记录
	vectorRecord := &VectorRecord{
		agentId: ms.BindingAgents[0],
		data:    vector,
	}

	// 添加到记忆空间
	ms.vectorUints = append(ms.vectorUints, vectorRecord)

	// 更新可用空间（假设每个float32占4字节）
	vectorSize := uint64(len(vector) * 4)
	if ms.availSpace+vectorSize <= ms.spaceLimit {
		ms.availSpace += vectorSize
	} else {
		// 空间不足，可以在这里实现LRU淘汰策略
		log.Printf("Warning: memory space approaching limit. Used: %d/%d",
			ms.availSpace, ms.spaceLimit)
	}

	log.Printf("Embedding generated: dimension=%d, content='%s'",
		len(vector), cleanedContent[:min(50, len(cleanedContent))])

	return vectorRecord, nil
}

// preClean 文本预处理
func (ms *MemSpace) preClean(content string) string {
	if content == "" {
		return ""
	}

	// 1. 转换为小写
	cleaned := strings.ToLower(content)

	// 2. 移除多余的空格和换行符
	cleaned = strings.Join(strings.Fields(cleaned), " ")

	// 3. 移除特殊字符，保留字母、数字、中文和基本标点
	cleaned = removeSpecialCharacters(cleaned)

	// 4. 移除首尾空格
	cleaned = strings.TrimSpace(cleaned)

	if cleaned == "" {
		return ""
	}

	// 5. 根据记忆空间类型进行特定清理
	switch ms.memSpaceContentType {
	case ToolMemory:
		// 工具记忆：保留代码相关符号但移除括号等
		cleaned = cleanForToolMemory(cleaned)
	case ContentMemory:
		// 内容记忆：移除所有非字母数字和中文的字符
		cleaned = cleanForContentMemory(cleaned)
	case BehavioralMemory:
		// 行为记忆：保留决策相关关键词
		cleaned = cleanForBehavioralMemory(cleaned)
	case EpisodicMemory:
		// 情景记忆：保留时间、地点等情景信息
		cleaned = cleanForEpisodicMemory(cleaned)
	}

	// 6. 简单的停用词过滤
	cleaned = removeStopWords(cleaned)

	// 7. 移除连续的标点符号和多余空格
	cleaned = removeRepeatedPunctuation(cleaned)
	cleaned = strings.Join(strings.Fields(cleaned), " ") // 再次清理空格

	return cleaned
}

// removeSpecialCharacters 移除特殊字符，保留字母、数字、中文和基本标点
func removeSpecialCharacters(text string) string {
	var result strings.Builder
	for _, r := range text {
		// 保留：字母、数字、空格、中文
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r == ' ') ||
			(r >= '\u4e00' && r <= '\u9fff') { // 中文范围
			result.WriteRune(r)
		}
	}
	return result.String()
}

// cleanForToolMemory 工具记忆的特定清理
func cleanForToolMemory(text string) string {
	var result strings.Builder
	for _, r := range text {
		// 保留字母、数字、空格
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r == ' ') {
			result.WriteRune(r)
		} else if r == ',' || r == '(' || r == ')' {
			// 对于逗号和括号，添加空格来分隔单词
			result.WriteRune(' ')
		}
	}

	// 清理多余的空格
	return strings.Join(strings.Fields(result.String()), " ")
}

// cleanForContentMemory 内容记忆的特定清理
func cleanForContentMemory(text string) string {
	var result strings.Builder
	for _, r := range text {
		// 只保留字母、数字、空格、中文
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r == ' ') ||
			(r >= '\u4e00' && r <= '\u9fff') {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// cleanForBehavioralMemory 行为记忆的特定清理
func cleanForBehavioralMemory(text string) string {
	// 行为记忆保留字母、数字、中文
	var result strings.Builder
	for _, r := range text {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r == ' ') ||
			(r >= '\u4e00' && r <= '\u9fff') {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// cleanForEpisodicMemory 情景记忆的特定清理
func cleanForEpisodicMemory(text string) string {
	// 情景记忆保留字母、数字、中文
	var result strings.Builder
	for _, r := range text {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r == ' ') ||
			(r >= '\u4e00' && r <= '\u9fff') {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// removeStopWords 移除停用词
func removeStopWords(text string) string {
	stopWords := []string{"的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都", "一", "一个", "上", "也", "很", "到", "说", "要", "去", "你", "会", "着", "没有", "看", "好", "自己", "这", "那", "但", "什么", "把", "又", "可以"}

	words := strings.Fields(text)
	var result []string

	for _, word := range words {
		isStopWord := false
		for _, stopWord := range stopWords {
			if word == stopWord {
				isStopWord = true
				break
			}
		}
		if !isStopWord && len(word) > 0 {
			result = append(result, word)
		}
	}

	return strings.Join(result, " ")
}

// removeRepeatedPunctuation 移除连续的标点符号
func removeRepeatedPunctuation(text string) string {
	// 由于我们已经移除了所有标点符号，这个函数现在只需要处理空格
	return strings.Join(strings.Fields(text), " ")
}

// cacSimilarity 计算两个向量记录的余弦相似度
func (ms *MemSpace) cacSimilarity(record1 VectorRecord, record2 VectorRecord) float32 {
	vector1 := record1.data
	vector2 := record2.data

	if len(vector1) != len(vector2) {
		log.Printf("Warning: vector dimension mismatch: %d vs %d",
			len(vector1), len(vector2))
		return 0
	}

	if len(vector1) == 0 {
		return 0
	}

	var dotProduct, norm1, norm2 float32
	for i := 0; i < len(vector1); i++ {
		dotProduct += vector1[i] * vector2[i]
		norm1 += vector1[i] * vector1[i]
		norm2 += vector2[i] * vector2[i]
	}

	if norm1 == 0 || norm2 == 0 {
		return 0
	}

	return dotProduct / (sqrt(norm1) * sqrt(norm2))
}

// getRecommendedDimensions 根据记忆空间类型推荐向量维度
func (ms *MemSpace) getRecommendedDimensions() int {
	switch ms.memSpaceContentType {
	case ToolMemory:
		return 512 // 工具记忆：中等维度
	case ContentMemory:
		return 1024 // 内容记忆：高维度保留语义信息
	case BehavioralMemory:
		return 768 // 行为记忆：中等偏高质量
	case EpisodicMemory:
		return 512 // 情景记忆：中等维度
	default:
		return 1024 // 默认高维度
	}
}

// BatchEmbedding 批量生成嵌入向量
func (ms *MemSpace) BatchEmbedding(contents []string) ([]*VectorRecord, error) {
	if ms.embeddingServerClient == nil {
		return nil, fmt.Errorf("embedding server client not initialized")
	}

	// 预处理所有文本
	var cleanedTexts []string
	for _, content := range contents {
		cleaned := ms.preClean(content)
		if cleaned != "" {
			cleanedTexts = append(cleanedTexts, cleaned)
		}
	}

	if len(cleanedTexts) == 0 {
		return nil, fmt.Errorf("no valid content after cleaning")
	}

	// 批量生成嵌入向量
	dimensions := ms.getRecommendedDimensions()
	embeddings, err := ms.embeddingServerClient.Embed(cleanedTexts, dimensions)
	if err != nil {
		return nil, fmt.Errorf("batch embedding failed: %v", err)
	}

	// 创建向量记录
	var records []*VectorRecord
	for _, embedding := range embeddings {
		record := &VectorRecord{
			agentId: ms.BindingAgents[0],
			data:    embedding,
		}
		records = append(records, record)

		// 更新空间使用
		vectorSize := uint64(len(embedding) * 4)
		if ms.availSpace+vectorSize <= ms.spaceLimit {
			ms.availSpace += vectorSize
		}
	}

	// 添加到记忆空间
	ms.vectorUints = append(ms.vectorUints, records...)

	log.Printf("Batch embedding completed: %d vectors generated", len(records))
	return records, nil
}

// GetVectorStats 获取向量统计信息
func (ms *MemSpace) GetVectorStats() map[string]interface{} {
	totalVectors := len(ms.vectorUints)
	totalDimensions := 0
	if totalVectors > 0 {
		totalDimensions = len(ms.vectorUints[0].data)
	}

	return map[string]interface{}{
		"total_vectors":    totalVectors,
		"vector_dimension": totalDimensions,
		"space_used":       ms.availSpace,
		"space_limit":      ms.spaceLimit,
		"space_usage":      fmt.Sprintf("%.2f%%", float64(ms.availSpace)/float64(ms.spaceLimit)*100),
	}
}

// 辅助函数
func sqrt(x float32) float32 {
	// 简单的平方根近似计算
	var z float32 = 1.0
	for i := 0; i < 10; i++ {
		z -= (z*z - x) / (2 * z)
	}
	return z
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
