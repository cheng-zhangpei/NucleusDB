package search

import (
	"ComDB"
)

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
	CompressThreshold         int64                // 是否压缩的阈值
	ms                        *MemoryStructure     // 用于操作记忆空间
}

// NewCompressor 创建压缩结构
func NewCompressor(opts ComDB.CompressOptions, agentId string, ms *MemoryStructure) (*Compressor, error) {
	// 读取元数据
	mm, err := ms.FindMetaData([]byte(agentId))
	if err != nil {
		return nil, err
	}
	// 计算不同节点的相似度
	ms.mm = mm
	ms.mm.mu.Lock() // 不加锁可能会导致拿出的相似度发生不一致的问题
	defer ms.mm.mu.Unlock()
	similarities, err := ms.GetSimilarities()
	if err != nil {
		return nil, err
	}
	return &Compressor{
		ComPressNumThreshold:      opts.ComPressNumThreshold,
		CompressCleanThreshold:    opts.CompressCleanThreshold,
		CompressHighSimDis:        opts.CompressHighSimDis,
		CompressThreshold:         opts.CompressNum,
		SimilarityCoefficientList: similarities,
		CompressCoefficientList:   make(map[string]float64),
		ms:                        ms,
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
func (cs *Compressor) Compress(agentID string, endpoint string) (bool, error) {
	// 判断是否到达压缩阈值
	valid, CompressCoefficient := cs.getCurrentSimilarityLen(cs.SimilarityCoefficientList)
	if !valid {
		return false, ComDB.ErrMemoryMetaNotFound
	}
	// 用于存储当前批次的数据
	batch := make([]string, cs.CompressThreshold)
	CompressCoefficientList := make([]float64, cs.CompressThreshold)
	batchSize := 0
	index := 0
	// 遍历 CompressCoefficient，分批次压缩
	for key, value := range CompressCoefficient {
		// key 是realKey。 value是压缩系数
		batchSize++
		batch[index] = key
		CompressCoefficientList[index] = value
		index += 1
		// 如果批次大小达到 CompressNum，则进行压缩
		if batchSize >= int(cs.CompressThreshold) {
			// 调用压缩函数进行压缩
			compressedData, err := cs.handleCompressedData(batch,
				CompressCoefficientList, agentID, endpoint)
			if err != nil {
				return false, err // 压缩失败，返回错误
			}
			// 处理压缩后的数据（例如存储或更新索引）
			err = cs.storeCompressedData(compressedData)
			if err != nil {
				return false, err // 处理失败，返回错误
			}
			// 重置批次
			batch = make([]string, len(CompressCoefficient))
			batchSize = 0
		}
	}
	return false, nil
}

// 返回符合要求的长度列表以及对应关系,这里的string是realKey
func (cs *Compressor) getCurrentSimilarityLen(similarities map[string][]float64) (bool, map[string]float64) {
	var currentCur int64 = 0
	memoryNode := make(map[string]float64)
	for realKey, similarity := range similarities {
		if len(similarity) == int(cs.ComPressNumThreshold) {
			// 计算压缩系数之前还需要判断是否符合相似度数组满的情况
			currentCur++
			// 计算压缩系数   lambda默认值是0.1
			compressCoefficient := getCompressCoefficient(similarity, 0.1)
			memoryNode[realKey] = compressCoefficient
		}
	}
	//
	if currentCur < cs.CompressThreshold {
		return false, memoryNode
	}
	return true, memoryNode
}

// handleCompressedData 处理压缩数据，compressedKey是realKey
func (cs *Compressor) handleCompressedData(compressedKey []string, CompressCoefficientList []float64,
	agentID string, endpoint string) (string, error) {
	// 取出真实数据
	index := 0
	tempValue := make([]string, cs.CompressThreshold)
	for _, realKey := range compressedKey {
		value, err := cs.ms.Db.Get([]byte(realKey))
		// 如果拿取出问题
		if err != nil {
			return "", err
		}
		// 解析searchRecord
		record, err := DecodeSearchRecord(value)
		if err != nil {
			return "", err
		}
		data := string(record.dataField)
		tempValue[index] = data
		index += 1
	}
	// 压缩数据-> 根据CompressCoefficientList中的值制定权重
	// 压缩数据（使用加权平均）
	compressedWeight, err := cs.calcWeights(tempValue, CompressCoefficientList)
	if err != nil {
		return "", err
	}
	// 获取token数量分配
	tokenDistribute := getTokenDistribute(compressedWeight, tempValue)
	// 将token数量以及prompt发送给模型
	_, err = getLLMResponse(tempValue, tokenDistribute, endpoint)
	if err != nil {
		return "", err
	}
	// 解析模型输出：将压缩数据拼接到一起 todo 先看看这个数据解析出来是啥玩意

	// 储存压缩数据

	return "", nil
}

// storeCompressedData 将压缩之后的记忆重新插入记忆空间
func (cs *Compressor) storeCompressedData(compressedData string) error {
	return nil
}

// 获得了压缩系数之后权重的权重计算
// todo 需要根据数据的长度与压缩系数来决定权重，不能直接通过压缩系数的权重决定会导致记忆倾斜的问题
func (cs *Compressor) calcWeights(data []string, weights []float64) ([]float64, error) {

	return weights, nil
}
