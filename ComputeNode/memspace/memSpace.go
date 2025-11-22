package memspace

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
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
	stopFlush             chan struct{} // the chan to notify goroutine when to stop
	flushTime             int
	mu                    *sync.RWMutex
	tempIndexPtr          uint64 // the latest/update position of the temp memory space
	tempSpaceSize         uint64 // indicate the size of the temp memSpace
	persistKey            string // record the area the memspace persist the memUint
	dbClient              *NucleusClient

	eventChan chan<- MemEvent
}

func NewMemSpace(id uint64, spaceType MemSpaceType,
	spaceLimit uint64, memSpaceContentType MemSpaceContentType,
	embeddingServerAddr string, flushTime int, tempMemoSize uint64, MemSpacePersistKey string,
	dbClient *NucleusClient, memEventChan chan MemEvent) *MemSpace {
	embeddingClient := NewEmbeddingServerClient(embeddingServerAddr)
	ms := &MemSpace{
		MemSpaceId: id,
		// todo 这些空间的大小限制还没有作
		BindingAgents: make([]uint64, 0),
		// todo 这里我是否可以搞一个类似冷热分层? 后续有空再来吧
		memUints:            make([]*MemUint, 0),
		TempMemUnits:        make([]*TempMemUnit, tempMemoSize),
		vectorUints:         make([]*VectorRecord, 0),
		spaceType:           spaceType,
		spaceStatus:         Pending,
		spaceLimit:          spaceLimit,
		availSpace:          0,
		memSpaceContentType: memSpaceContentType,
		stopFlush:           make(chan struct{}),
		//computeMetric: &compute.QualityMetrics{},
		embeddingServerClient: embeddingClient,
		flushTime:             flushTime,
		mu:                    new(sync.RWMutex),
		tempIndexPtr:          0,
		tempSpaceSize:         tempMemoSize,
		persistKey:            MemSpacePersistKey,
		dbClient:              dbClient,
		eventChan:             memEventChan,
	}

	go ms.startFlushRoutine(ms.flushTime)
	return ms
}
func (ms *MemSpace) startFlushRoutine(intervalMs int) {
	interval := time.Duration(intervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := ms.flush()
			if err != nil {
				fmt.Printf("Flush failed: %v\n", err)
			}
		case <-ms.stopFlush:
			fmt.Println("Flushing routine stopped.")
			return
		}
	}
}

// 模拟将 TempMemUnits 刷入数据库的函数（你来实现具体逻辑）
func (ms *MemSpace) flush() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	// 如果没有数据，跳过
	if ms.TempMemUnits[0] == nil {
		return nil
	}

	fmt.Printf("[flush go routine][%s] Flushing temporary memory units to DB...\n", getCurrentTimeString())
	for _, unit := range ms.TempMemUnits {
		if unit != nil {
			timeStamp := unit.Timestamp
			content := unit.Value
			persistUint := []byte(fmt.Sprintf("[time(ms): %d]: %s", timeStamp, content))
			// todo 这里刷新需要如何处理共享卷？
			persistKey := fmt.Sprintf("[%d] %s", timeStamp, ms.persistKey)
			err := ms.PersistMemoryUint(persistKey, persistUint)
			if err != nil {
				return err
			}
			// modify the meta data of the memspace
			ms.NotifyManager()
		} else {
			break
		}
	}
	return nil
}

func (ms *MemSpace) StopFlushRoutine() {
	close(ms.stopFlush)
}

// ---------------------------Persist memory operation: I want this part focus on memory record operations----------------------------

func (ms *MemSpace) SaveTempMemory(content string, agentId uint64) error {
	if !ms.checkAuthority() {
		return fmt.Errorf("authority check failed!please check permission of the agent %d", agentId)
	}
	if content == "" {
		return fmt.Errorf("content is empty")
	}
	cleanedContent := ms.preClean(content)
	// check tempIndex boundary
	if ms.tempIndexPtr == ms.tempSpaceSize-1 {
		ms.tempIndexPtr = 0
	}
	ms.TempMemUnits[ms.tempIndexPtr] = &TempMemUnit{cleanedContent, uint64(time.Now().Unix())}
	return nil
}
func (ms *MemSpace) GetTempSpaceMemory() string {
	var result string = ""
	for _, unit := range ms.TempMemUnits {
		if unit != nil {
			value := unit.Value
			timestamp := unit.Timestamp
			result = fmt.Sprintf("%s+[TimeStamp:%d][content:%s]", result, timestamp, value)
		} else {
			break
		}
	}
	return result
}
func (ms *MemSpace) GetPersistMemoryUint(key string) ([]byte, error) {
	return nil, nil
}
func (ms *MemSpace) UpdatePersistMemory(key string, data []byte) error {
	return nil
}

// PersistMemoryUint persist tempMemUint
func (ms *MemSpace) PersistMemoryUint(key string, data []byte) error {
	// we only need to persist a single unit no need to use transaction? you need to modify the mate data

	err := ms.dbClient.DistributePut([]byte(key), data)

	if err != nil {
		return err
	}
	return nil
}
func (ms *MemSpace) DeletePersistMemory(key string) error {
	return nil
}
func (ms *MemSpace) ListPersistMemories() []string {
	return nil
}

// ---------------------------agent operation----------------------------
// todo should the memspace have tha ability to bind the agent?

func (ms *MemSpace) GetBoundAgents() []uint64 {
	return ms.BindingAgents
}
func (ms *MemSpace) IsBounded(agentID uint64) bool {
	return false
}

// canBinding space can binding? todo: authority check
func (ms *MemSpace) canBinding() bool {
	return true
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

func (ms *MemSpace) checkAuthority() bool {
	return true
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

// NotifyManager inform the manager which part should be modified in metaData
func (ms *MemSpace) NotifyManager() {
	// 加读锁，安全读取当前状态
	ms.mu.RLock()
	event := MemEvent{
		MemSpaceId:          ms.MemSpaceId,
		spaceType:           ms.spaceType,
		spaceStatus:         ms.spaceStatus,
		spaceLimit:          ms.spaceLimit,
		availSpace:          ms.availSpace,
		description:         ms.description,
		name:                ms.name,
		memSpaceContentType: ms.memSpaceContentType,
		flushTime:           ms.flushTime,
		tempIndexPtr:        ms.tempIndexPtr,
		tempSpaceSize:       ms.tempSpaceSize,
		persistKey:          ms.persistKey,
	}
	ms.mu.RUnlock()

	// 发送事件（非阻塞）
	select {
	case ms.eventChan <- event:
		// 成功发送
	default:
		// channel 满了，丢弃事件（可选：记录日志）
		// log.Printf("event channel full, dropped update for MemSpace %d", ms.MemSpaceId)
	}
}
