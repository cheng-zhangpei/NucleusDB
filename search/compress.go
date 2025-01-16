package search

import (
	"ComDB"
	"errors"
	"math"
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
	if ms.Mm == nil {
		mm, err := ms.FindMetaData([]byte(agentId))
		if err != nil {
			return nil, err
		}
		// 计算不同节点的相似度
		ms.Mm = mm
	}
	ms.Mm.mu.Lock() // 不加锁可能会导致拿出的相似度发生不一致的问题
	defer ms.Mm.mu.Unlock()
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
		return false, ComDB.ErrCompressNumNotEnough
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
		if key == "" {
			break
		}
		if batchSize == int(cs.CompressThreshold) {
			// 调用压缩函数进行压缩
			compressedData, err := cs.handleCompressedData(batch,
				CompressCoefficientList, agentID, endpoint)
			if err != nil {
				return false, err // 压缩失败，返回错误
			}
			// 处理压缩后的数据（例如存储或更新索引）
			err = cs.storeCompressedData(compressedData, batch)
			if err != nil {
				return false, err // 处理失败，返回错误
			}
			// 重置批次
			batch = make([]string, cs.CompressThreshold)
			batchSize = 0
			// 索引别忘记重置否则会导致越界
			index = 0
		}
	}
	return true, nil
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
func (cs *Compressor) handleCompressedData(compressedKey []string,
	CompressCoefficientList []float64,
	agentID string, endpoint string) (string, error) {
	// 取出真实数据
	index := 0
	tempValue := make([]string, cs.CompressThreshold)
	realKeys := make([]string, cs.CompressThreshold)
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
		realKeys[index] = realKey
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
	modelResponse, err := getLLMResponse(tempValue, tokenDistribute, endpoint)
	if err != nil {
		return "", err
	}
	// 解析模型输出：将压缩数据拼接到一起 todo 先看看这个数据解析出来是啥玩意
	var compressedData string = ""
	for _, response := range modelResponse {
		compressed, err := modelDecode(response)
		if err != nil {
			return "", err
		}
		compressedData += compressed
		compressedData += "\n"
	}
	return compressedData, nil
}

// storeCompressedData 将压缩之后的记忆重新插入记忆空间
func (cs *Compressor) storeCompressedData(compressedData string, realKeys []string) error {
	// 删除记忆空间中的数据
	// 解码realKey
	for _, realKey := range realKeys {
		timestamp, _, err := decodeSearchKey([]byte(realKey))
		if err != nil {
			return err
		}
		// 这里metaData已经装载了，需要压缩的信息也有可能被其他的记忆替换出了记忆空间，所以找不到searchKey是非常正常的
		// 但是我总感觉整个数据库的数据更新机制有点问题
		err = cs.ms.Mm.RemoveTimestamp(timestamp)
		// 删除SearchRecord
		err = cs.ms.Db.Delete([]byte(realKey))
		if err != nil {
			return err
		}
		if errors.Is(err, ComDB.ErrTimestampNotExist) {
			// 这个地方如果timestamp不存在不一定是有问题。可能是被记忆空间所剔除不影响压缩数据的插入
			continue
		}
	}
	// 保存压缩之后的数据
	err := cs.ms.MMSet([]byte(compressedData), cs.ms.Mm.agentId)
	if err != nil {
		return err
	}
	//// 测试一下数据丢进去了不
	//timestamps := cs.ms.mm.GetAllMemory()
	//for _, timestamp := range timestamps {
	//	key := getSearchKey(timestamp, cs.ms.mm.agentId)
	//	get, err := cs.ms.Db.Get([]byte(key))
	//	if err != nil {
	//		return err
	//	}
	//	record, err := DecodeSearchRecord(get)
	//	if err != nil {
	//		return err
	//	}
	//	fmt.Println(string(record.dataField))
	//	fmt.Println("------------------------------------------")
	//}

	return nil
}

// 获得了压缩系数之后权重的权重计算
func (cs *Compressor) calcWeights(data []string, coefficient []float64) ([]float64, error) {
	weights := make([]float64, len(data))
	totalWeight := 0.0

	// 归一化 coefficient
	maxCoefficient := getMax(coefficient)
	normalizedCoefficient := make([]float64, len(coefficient))
	for i := 0; i < len(coefficient); i++ {
		normalizedCoefficient[i] = coefficient[i] / maxCoefficient
	}

	// 计算每个数据项的长度权重
	lengthWeights := make([]float64, len(data))
	maxLength := getMaxLength(data)
	for i := 0; i < len(data); i++ {
		lengthWeights[i] = math.Sqrt(float64(len(data[i]))) / math.Sqrt(float64(maxLength)) // 归一化长度权重
	}

	// 计算综合权重
	for i := 0; i < len(data); i++ {
		weights[i] = normalizedCoefficient[i] * lengthWeights[i]
		totalWeight += weights[i]
	}

	// 归一化权重
	for i := 0; i < len(weights); i++ {
		weights[i] = weights[i] / totalWeight
	}

	return weights, nil
}

// 获取最大值
func getMax(arr []float64) float64 {
	max := arr[0]
	for _, v := range arr {
		if v > max {
			max = v
		}
	}
	return max
}

// 获取最大长度
func getMaxLength(data []string) int {
	max := len(data[0])
	for _, s := range data {
		if len(s) > max {
			max = len(s)
		}
	}
	return max
}
