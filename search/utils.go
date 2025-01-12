package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
)

// getCompressCoefficient 计算压缩系数，由于时间相似性的值比较小，压缩系数的值就会比较大
func getCompressCoefficient(similarity []float64, lambda float64) float64 {
	// 检查输入是否为空
	if len(similarity) == 0 {
		return 1.0 // 如果没有相似度数据，返回最大压缩系数
	}

	// 计算加权和
	weightedSum := 0.0
	for i := 0; i < len(similarity); i++ {
		// 使用索引作为时间戳的替代
		timestamp := float64(i) // 索引越小，表示记忆内容越新

		// 计算时间衰减权重
		timeWeight := lambda * math.Exp(-lambda*timestamp)

		// 累加加权相似度
		weightedSum += similarity[i] * timeWeight
	}

	// 计算压缩系数
	if weightedSum == 0 {
		return 1.0 // 如果加权和为 0，返回最大压缩系数
	}
	compressCoefficient := 1 / weightedSum

	return compressCoefficient
}

// 获得token的分布
func getTokenDistribute(weights []float64, tempData []string) []int {
	// 检查输入长度是否一致
	if len(weights) != len(tempData) {
		return nil
	}
	// 计算每个数据项的 token 数量
	tokenLengths, maxToken := getTokenLengths(tempData)

	// 计算加权 token 数量
	weightedTokenLengths := make([]float64, len(weights))
	totalWeightedTokenLength := 0.0
	for i := 0; i < len(weights); i++ {
		weightedTokenLengths[i] = weights[i] * float64(tokenLengths[i])
		totalWeightedTokenLength += weightedTokenLengths[i]
	}

	// 初始化 token 分配结果
	tokenDistribute := make([]int, len(weights))

	// 根据加权 token 数量分配 token
	for i := 0; i < len(weights); i++ {
		tokenDistribute[i] = int(float64(maxToken) * (weightedTokenLengths[i] / totalWeightedTokenLength))
	}
	// 这样会使得有价值但是比较短的记忆被扩展，比较长但是权重低的记忆被压缩
	// todo 但是如果遇到极端情况怎么办
	return tokenDistribute
}

// 获得token的长度
func getTokenLengths(tempData []string) ([]int, int) {
	tokenLengths := make([]int, len(tempData))
	max_len := 0

	for i, data := range tempData {
		// 这里暂时用字符串的长度作为token数量
		tokenLengths[i] = len(data)
		if len(data) > max_len {
			max_len = len(data)
		}
	}
	return tokenLengths, max_len
}

// LLMResponse 定义 LLM 的响应结构
type LLMResponse struct {
	Result string `json:"result"`
}

// getLLMResponse 限制并获取小模型输出
func getLLMResponse(data []string, tokenDistribute []int, endpoint string) ([]string, error) {
	// 检查输入长度是否一致
	if len(data) != len(tokenDistribute) {
		return nil, fmt.Errorf("data 和 tokenDistribute 长度不匹配")
	}

	// 存储所有响应
	responses := make([]string, len(data))
	// 遍历每个数据项
	for i, text := range data {
		// 构造prompt
		prompt := buildPrompt(text, tokenDistribute[i])
		// 构造请求体
		requestBody := map[string]interface{}{
			"message":    prompt,
			"max_tokens": tokenDistribute[i], // 使用分配的 token 数量
		}
		requestBodyBytes, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("无法序列化请求体: %v", err)
		}

		// 发送 HTTP POST 请求
		resp, err := http.Post(
			endpoint, // LLM 服务的地址
			"application/json",
			bytes.NewBuffer(requestBodyBytes),
		)
		if err != nil {
			return nil, fmt.Errorf("请求 LLM 服务失败: %v", err)
		}
		defer resp.Body.Close()

		// 读取响应体
		responseBody, err := io.ReadAll(resp.Body) // 使用 io.ReadAll 替代 ioutil.ReadAll
		if err != nil {
			return nil, fmt.Errorf("读取响应体失败: %v", err)
		}

		// 解析响应体
		var llmResponse LLMResponse
		if err := json.Unmarshal(responseBody, &llmResponse); err != nil {
			return nil, fmt.Errorf("解析响应体失败: %v", err)
		}
		// 存储响应结果
		responses[i] = llmResponse.Result
	}
	return responses, nil
}

// buildPrompt 构建用于压缩或扩充数据的 prompt
func buildPrompt(text string, compressToken int) string {
	// 构造请求体
	request := map[string]interface{}{
		"data":           text,
		"compress_token": compressToken,
	}

	// 将请求体转换为 JSON 字符串
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return "" // 如果序列化失败，返回空字符串
	}

	// 构建完整的 prompt
	prompt := fmt.Sprintf(`
# 我需要你对下面给的 data 数据进行压缩或者扩充。
# compress_token 参数是你需要将 data 扩充或者压缩到的长度，将压缩的结果放到 response 的 compressed_data 字段中。
# request part
%s
# response
# 下面是你的回复，你只需要回复下面的括号中的内容也就是 json 结构体。
{
    "compressed_data": "xxx"
}`, string(requestJSON))

	return prompt
}

// similaritiesUpdate 用于记忆空间相似的状态解析
func similaritiesUpdate(index int, isFull bool, threshold int64, similarities []float64, similarity float64) ([]float64, int, bool) {
	// 创建新数组
	updateSimilarities := make([]float64, len(similarities))
	// 需要将update数组和similarities对齐
	updateSimilarities = similarities
	// 1. 空间未满，此时 index 指向更新位置
	if !isFull {
		updateSimilarities[index] = similarity
		index++

		// 如果 index 达到 threshold，标记为已满，并重置 index
		if index >= int(threshold) {
			isFull = true
			index = 0
		}
	} else {
		// 2. 空间已满，循环替换
		updateSimilarities[index] = similarity
		index++
		// 如果 index 达到 threshold，重置为 0
		if index >= int(threshold) {
			index = 0
		}
	}

	return updateSimilarities, index, isFull
}
