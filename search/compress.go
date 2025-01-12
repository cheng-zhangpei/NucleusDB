package search

import "ComDB"

// 记忆空间压缩 -> 如何合理的进行agent记忆空间的压缩？ -> 目的就是在
// 这个结构需要从记忆空间中将数据读取出来，独立进行压缩，虽然会占用更多的内存但是解耦会稍微好一点

// 这个压缩的相关系数并不需要持久化
// 压缩功能是否启用由用户决定

type Compressor struct {
	SimilarityCoefficientList map[string][]float64 // 相似性记录
	CompressCoefficientList   map[string]float64   // 压缩系数记录
	ComPressNumThreshold      int64                // 该记忆空间压缩
	CompressHighSimDis        float64              // 高相似压缩距离
	CompressCleanThreshold    float64              // 高压缩清理系数
}

// NewCompressor 创建压缩结构
func NewCompressor(opts ComDB.CompressOptions, agentId string) (*Compressor, error) {
	// 读取元数据
	dbOpts := ComDB.DefaultOptions
	ms, err := NewMemoryStructure(dbOpts, agentId, 10)
	if err != nil {
		return nil, err
	}
	mm, err := ms.FindMetaData([]byte(agentId))
	// 计算不同节点的相似度
	ms.mm = mm
	ms.mm.mu.Lock() // 不加锁可能会导致拿出的相似度发生不一致的问题
	defer ms.mm.mu.Unlock()
	similarities, err := ms.GetSimilarities()
	return &Compressor{
		ComPressNumThreshold:      opts.ComPressNumThreshold,
		CompressCleanThreshold:    opts.CompressCleanThreshold,
		CompressHighSimDis:        opts.CompressHighSimDis,
		SimilarityCoefficientList: similarities,
		CompressCoefficientList:   make(map[string]float64),
	}, nil
}

// CompressHigh 将多个有高度相似匹配区的记忆数据进行合并 -> 由压缩系数决定，压缩系数相近说明数据重叠性非常高
func (cs *Compressor) CompressHigh() (bool, error) {
	return false, nil
}

// CompressClean 清理相似度过低的记忆节点
func (cs *Compressor) CompressClean() (bool, error) {
	return false, nil
}

// Compress 一旦达到阈值，按照阈值对节点进行压缩
func (cs *Compressor) Compress() (bool, error) {
	return false, nil
}
