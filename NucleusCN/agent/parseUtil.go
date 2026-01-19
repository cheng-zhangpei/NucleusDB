package agent

import (
	"encoding/json"
	"log"
	"strings"
)

// AgentResponse 对应 Prompt 要求的 JSON 输出结构
type AgentResponse struct {
	Content     string `json:"content"`      // 核心回复内容
	TargetTopic string `json:"target_topic"` // 路由目标 Topic
	TargetAgent string `json:"target_agent"` // 路由目标 Agent (新增字段)
	TargetSpace string `json:"target_space"` // 记忆空间的key
}

func parseLLMResponse(rawResponse string) *AgentResponse {
	// 1. 预处理：去除首尾空白
	cleaned := strings.TrimSpace(rawResponse)

	// 2. 尝试提取 JSON 部分
	// 很多时候 LLM 会输出: "Here is the JSON:\n```json\n{...}\n```"
	// 我们尝试找到第一个 '{' 和最后一个 '}' 包裹的内容
	start := strings.Index(cleaned, "{")
	end := strings.LastIndex(cleaned, "}")

	if start != -1 && end != -1 && end > start {
		cleaned = cleaned[start : end+1]
	} else {
		// 如果找不到花括号，可能格式完全错了，尝试清理 Markdown 标记再试一次
		cleaned = strings.TrimPrefix(cleaned, "```json")
		cleaned = strings.TrimPrefix(cleaned, "```")
		cleaned = strings.TrimSuffix(cleaned, "```")
		cleaned = strings.TrimSpace(cleaned)
	}

	// 3. 反序列化
	var resp AgentResponse
	err := json.Unmarshal([]byte(cleaned), &resp)
	if err != nil {
		log.Printf("[Agent Parse Error] Failed to unmarshal JSON: %v. Raw: %s", err, rawResponse)

		// 4. 解析失败的兜底策略 (Fallback)
		// 如果解析失败，我们假设整个 rawResponse 都是 content，并路由到通用频道
		return &AgentResponse{
			Content:     rawResponse, // 保留原始回复
			TargetTopic: "nil",       // 默认 Topic
			TargetAgent: "nil",       // 默认 Agent
			TargetSpace: "nil",       // 默认 Agent
		}
	}

	// 5. 字段默认值修正
	// 如果 LLM 漏填了某些字段，给它们默认值
	if resp.TargetTopic == "" {
		resp.TargetTopic = "nil"
	}
	if resp.TargetAgent == "" {
		resp.TargetAgent = "nil"
	}
	if resp.TargetSpace == "" {
		resp.TargetSpace = "nil"
	}
	return &resp
}

func MarshalCommunicationUnits(units []*CommunicationUnit) (string, error) {
	data, err := json.Marshal(units)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
