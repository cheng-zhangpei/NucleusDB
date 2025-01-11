package search

import "ComDB/search/match_method"

type MatchMethod int8

const (
	TF_IDF MatchMethod = iota // TF-IDF 方法
	BM25                      // BM25 方法
)

// Match 接口定义
type Matcher interface {
	Store(memory string)          // 存储记忆
	Match(query string) []float64 // 计算查询与记忆的相似度
	GenerateMatches(text string) map[string]float64
}

func NewMatcher(matchType MatchMethod) (Matcher, error) {
	switch matchType {
	case TF_IDF:
		return match_method.NewTFIDFMatcher(), nil
	default:
		panic("unhandled match type!")
	}
}
