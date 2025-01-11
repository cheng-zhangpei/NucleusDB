package match_method

import (
	"github.com/yanyiwu/gojieba" // GoJieba 分词库
	"math"
)

// DocumentFrequency 用于存储单词的文档频率
type DocumentFrequency map[string]int

// TFIDFMatcher 实现了基于 TF-IDF 的匹配器
type TFIDFMatcher struct {
	documents []string          // 存储的文档集合
	df        DocumentFrequency // 文档频率统计
	totalDocs int               // 总文档数
	jieba     *gojieba.Jieba    // GoJieba 分词器
}

// NewTFIDFMatcher 创建一个新的 TFIDFMatcher
func NewTFIDFMatcher() *TFIDFMatcher {
	return &TFIDFMatcher{
		documents: []string{},
		df:        make(DocumentFrequency),
		jieba:     gojieba.NewJieba(),
	}
}

// Store 存储文档并更新文档频率
func (m *TFIDFMatcher) Store(document string) {
	m.documents = append(m.documents, document)
	m.totalDocs++

	// 更新文档频率
	words := m.tokenize(document)
	uniqueWords := make(map[string]bool)
	for _, word := range words {
		uniqueWords[word] = true
	}
	for word := range uniqueWords {
		m.df[word]++
	}
}

// Match 计算查询与文档的相似度
func (m *TFIDFMatcher) Match(query string) []float64 {
	queryTfidf := m.GenerateMatches(query)
	similarities := make([]float64, len(m.documents))

	for i, doc := range m.documents {
		docTfidf := m.GenerateMatches(doc)
		similarities[i] = cosineSimilarity(queryTfidf, docTfidf)
	}

	return similarities
}

// tokenize 使用 GoJieba 对文本进行分词
func (m *TFIDFMatcher) tokenize(text string) []string {
	return m.jieba.Cut(text, true)
}

// GenerateMatches 生成 TF-IDF 向量
func (m *TFIDFMatcher) GenerateMatches(text string) map[string]float64 {
	tf := m.calculateTF(text)
	matches := make(map[string]float64)

	for word, tfValue := range tf {
		idf := m.calculateIDF(word)
		matches[word] = tfValue * idf
	}

	return matches
}

// calculateTF 计算词频（TF）
func (m *TFIDFMatcher) calculateTF(text string) map[string]float64 {
	words := m.tokenize(text)
	tf := make(map[string]float64)
	totalWords := float64(len(words))

	for _, word := range words {
		tf[word]++
	}

	for word, count := range tf {
		tf[word] = count / totalWords
	}

	return tf
}

// calculateIDF 计算逆文档频率（IDF）
func (m *TFIDFMatcher) calculateIDF(word string) float64 {
	return math.Log(float64(m.totalDocs) / float64(m.df[word]+1))
}

// cosineSimilarity 计算两个向量的余弦相似度
func cosineSimilarity(vec1, vec2 map[string]float64) float64 {
	dotProduct := 0.0
	magnitude1 := 0.0
	magnitude2 := 0.0

	for word, val1 := range vec1 {
		if val2, exists := vec2[word]; exists {
			dotProduct += val1 * val2
		}
		magnitude1 += val1 * val1
	}
	for _, val2 := range vec2 {
		magnitude2 += val2 * val2
	}

	if magnitude1 == 0 || magnitude2 == 0 {
		return 0
	}
	return dotProduct / (math.Sqrt(magnitude1) * math.Sqrt(magnitude2))
}
