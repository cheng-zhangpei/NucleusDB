package memspace

import (
	"ComputeNode/msg"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ChannelDescriptor 描述一个信道（Topic）的语义元数据和特征
// 这是实现“自适应路由”的核心结构
type ChannelDescriptor struct {
	Topic       string
	Description string // Slow Path: 自然语言描述，用于提供给 LLM 进行决策。例如："讨论后端数据库死锁问题"

	// Fast Path: 启发式规则/特征缓存
	Keywords []string // 关键词特征，例如：["deadlock", "mysql", "slow query"]

	// 统计信息 (用于后续优化算法)
	MessageCount uint64
	LastActive   time.Time
}

// Watcher manages communication routing within a shared MemSpace.
type Watcher struct {
	// mu protects the routing tables
	mu sync.RWMutex
	// 1. Point-to-Point Routing Table (Physical Layer)
	p2pChannels map[uint64]chan *msg.AgentMsg
	// 2. Multicast/Topic Routing Table (Logical Layer)
	topicSubscribers map[string][]uint64
	// 3. Channel Metadata Registry (Semantic Layer)
	// Key: Topic string -> Value: Descriptor
	topicMetadata map[string]*ChannelDescriptor
	// 4. Routing Cache (Fast Path Layer)
	// Key: Content Feature Hash (or simple keyword) -> Value: Target Topic
	routeCache map[string]string
}

// ChannelInfo 用于序列化输出
type ChannelInfo struct {
	Topic       string `json:"topic"`
	Description string `json:"description"`
}

// NewWatcher creates a new Watcher instance
func NewWatcher() *Watcher {
	return &Watcher{
		p2pChannels:      make(map[uint64]chan *msg.AgentMsg),
		topicSubscribers: make(map[string][]uint64),
		topicMetadata:    make(map[string]*ChannelDescriptor), // init
		routeCache:       make(map[string]string),             // init
	}
}

// ---------------------- Channel Management (P2P Basis) ----------------------

func (w *Watcher) RegisterAgentChannel(agentID uint64, ch chan *msg.AgentMsg) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.p2pChannels[agentID]; exists {
		return fmt.Errorf("agent %d channel already registered", agentID)
	}
	w.p2pChannels[agentID] = ch
	return nil
}

func (w *Watcher) UnRegisterAgentChannel(agentID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.p2pChannels, agentID)

	// Clean up subscriptions
	for topic, subscribers := range w.topicSubscribers {
		var newSubs []uint64
		for _, subID := range subscribers {
			if subID != agentID {
				newSubs = append(newSubs, subID)
			}
		}
		if len(newSubs) == 0 {
			delete(w.topicSubscribers, topic)
		} else {
			w.topicSubscribers[topic] = newSubs
		}
	}
	return nil
}

// ---------------------- Topic Subscription & Metadata ----------------------

// SubscribeTopic 允许 Agent 订阅 Topic
func (w *Watcher) SubscribeTopic(agentID uint64, topic string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.p2pChannels[agentID]; !ok {
		return fmt.Errorf("agent %d not registered", agentID)
	}

	subs := w.topicSubscribers[topic]
	for _, subID := range subs {
		if subID == agentID {
			return nil
		}
	}
	w.topicSubscribers[topic] = append(subs, agentID)
	return nil
}

// RegisterTopicMetadata 允许注册 Topic 的语义描述 (Slow Path 的基础)
// 例如：RegisterTopicMetadata("db_log", "用于接收所有数据库层面的报错和慢查询日志")
func (w *Watcher) RegisterTopicMetadata(topic string, description string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.topicMetadata[topic]; !exists {
		w.topicMetadata[topic] = &ChannelDescriptor{
			Topic:       topic,
			Description: description,
			Keywords:    make([]string, 0), // 初始为空，后续通过 Learn 填充
			LastActive:  time.Now(),
		}
	} else {
		// Update description if needed
		w.topicMetadata[topic].Description = description
	}
}

// ---------------------- Messaging & Routing Logic ----------------------

// ResolveTargetTopic 核心路由决策函数 (The "Brain" of the Router)
// 返回: 目标 Topic, 是否命中 FastPath
func (w *Watcher) ResolveTargetTopic(content string) (string, bool) {
	w.mu.RLock()
	// 1. Fast Path Check: 查 routeCache 或简单的关键词匹配
	// (伪代码: 如果 content 包含 routeCache 里的 key，直接返回)
	if topic, hit := w.routeCache[content]; hit { // 实际应当使用 hash 或关键词匹配
		w.mu.RUnlock()
		return topic, true
	}
	w.mu.RUnlock()

	// 2. Slow Path Indicator: 返回空字符串
	// 意味着 Watcher 无法决定，需要上层 Agent 调用 LLM，根据 topicMetadata 中的 Description 进行决策
	return "", false
}

// LearnRoutingRule 反馈学习接口
// 当 LLM 完成了一次 Slow Path 决策后，调用此函数，让 Watcher 记住这次选择
// 从而将 Slow Path 转化为 Fast Path
func (w *Watcher) LearnRoutingRule(featureKeyword string, targetTopic string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. 更新 Cache
	w.routeCache[featureKeyword] = targetTopic

	// 2. 更新 Topic 的特征描述符 (沉淀知识)
	if desc, exists := w.topicMetadata[targetTopic]; exists {
		desc.Keywords = append(desc.Keywords, featureKeyword)
		desc.MessageCount++
	}
}

// ---------------------- Messaging Operations ----------------------

// SendP2P sends a message directly to a specific agent.
// 点对点发送：直接查 P2P 表。
func (w *Watcher) SendP2P(from, to uint64, msg *msg.AgentMsg) error {
	// 1. Find target channel
	// 2. Send non-blocking or with timeout
	return nil
}

// PublishTopic broadcasts a message to all agents subscribed to the topic.
func (w *Watcher) PublishTopic(from uint64, topic string, msg *msg.AgentMsg) error {
	// 1. Find all matching subscribers (Exact match or Prefix match)
	// 2. Loop through subscribers and send
	return nil
}

// BroadcastToAll sends a message to ALL registered agents (Use with caution!).
func (w *Watcher) BroadcastToAll(from uint64, msg *msg.AgentMsg) error {
	return nil
}

// ---------------------- Introspection ----------------------

// GenerateCommunicationMap generates a snapshot of the current routing topology.
// Helper function for debugging or for the LLM to understand the network.
func (w *Watcher) GenerateCommunicationMap() string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var channels []ChannelInfo

	// 遍历 Topic Metadata
	for topic, desc := range w.topicMetadata {
		channels = append(channels, ChannelInfo{
			Topic:       topic,
			Description: desc.Description,
		})
	}

	// 如果没有任何 Metadata，可能是还没注册，我们可以返回一些默认的或者空的 JSON
	if len(channels) == 0 {
		return "[]"
	}

	// 序列化为 JSON 字符串
	bytes, err := json.MarshalIndent(channels, "", "  ")
	if err != nil {
		return "[]" // Fallback
	}

	return string(bytes)
}
