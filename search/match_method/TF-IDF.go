package match_method

import (
	"math"
)
import "github.com/yanyiwu/gojieba"

// DocumentFrequency 用于存储单词的文档频率

// TFIDFMatcher 实现了基于 TF-IDF 的匹配器
type TFIDFMatcher struct {
	Documents []string       // 存储的文档集合
	Df        map[string]int // 文档频率统计
	TotalDocs int            // 总文档数
}

// NewTFIDFMatcher 创建一个新的 TFIDFMatcher
func NewTFIDFMatcher() *TFIDFMatcher {
	return &TFIDFMatcher{
		Documents: []string{},
		Df:        make(map[string]int),
		TotalDocs: 0,
	}
}

// Store 存储文档并更新文档频率
func (m *TFIDFMatcher) Store(document string, jieba *gojieba.Jieba) {
	m.Documents = append(m.Documents, document)
	m.TotalDocs++

	// 更新文档频率
	words := m.tokenize(document, jieba)
	uniqueWords := make(map[string]bool)
	for _, word := range words {
		uniqueWords[word] = true
	}
	for word := range uniqueWords {
		m.Df[word]++
	}
}

// Match 计算查询与文档的相似度
func (m *TFIDFMatcher) Match(vec1, vec2 map[string]float64) float64 {
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

// tokenize 使用 GoJieba 对文本进行分词
func (m *TFIDFMatcher) tokenize(text string, jieba *gojieba.Jieba) []string {
	return jieba.Cut(text, true)
}

// GenerateMatches 生成 TF-IDF 向量
func (m *TFIDFMatcher) GenerateMatches(text string, jieba *gojieba.Jieba) map[string]float64 {
	tf := m.calculateTF(text, jieba)
	matches := make(map[string]float64)

	for word, tfValue := range tf {
		idf := m.calculateIDF(word)
		matches[word] = tfValue * idf
	}

	return matches
}

// calculateTF 计算词频（TF）
func (m *TFIDFMatcher) calculateTF(text string, jieba *gojieba.Jieba) map[string]float64 {
	words := m.tokenize(text, jieba)
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
	// 边界处理：如果文档数量为 0，返回 0 并记录日志
	if m.TotalDocs == 0 {
		return 0
	}

	// 边界处理：如果单词未出现在任何文档中，返回 log(N + 1) 并记录日志
	if m.Df[word] == 0 {
		return math.Log(float64(m.TotalDocs) + 1)
	}

	// 正常计算 IDF
	return math.Log(float64(m.TotalDocs) / float64(m.Df[word]+1))
}
