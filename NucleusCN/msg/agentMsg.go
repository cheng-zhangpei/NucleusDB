package msg

import (
	"encoding/json"
)

// AgentMsg 定义智能体之间的通信协议
type AgentMsg struct {
	// 基础字段
	From    uint64 `json:"from"`    // 发送者 ID
	To      uint64 `json:"to"`      // 接收者 ID (如果是 0，表示可能是广播或组播)
	Topic   string `json:"topic"`   // 话题 (用于组播/语义广播)
	Content string `json:"content"` // 实际内容 (JSON String 或者 Plain Text)

	// 元数据
	Ts    int64  `json:"ts"`     // 发送时间戳 (Unix Nano)
	MsgID string `json:"msg_id"` // 消息唯一 ID (可选，用于去重)
}

// Serialize 序列化为字节数组
func (m *AgentMsg) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

// Deserialize 反序列化
func Deserialize(data []byte) (*AgentMsg, error) {
	var m AgentMsg
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}
