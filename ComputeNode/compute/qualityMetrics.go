package compute

// ComputeSpace some metric gotten from computation
type QualityMetrics struct {
	CompressionRatio   float64 // 压缩率（压缩后大小/原始大小）
	InformationDensity float64 // 信息密度评分
	NoiseLevel         float64 // 噪声水平（基于相似度分析）
	ConsistencyScore   float64 // 一致性评分（与其他相关记忆的一致性）
	RelevanceScore     float64 // 相关性评分（与智能体任务的相关性）
	ConfidenceScore    float64 // 置信度评分（基于历史使用效果）
}

func NewComputeSpace() *QualityMetrics {
	return new(QualityMetrics)
}
