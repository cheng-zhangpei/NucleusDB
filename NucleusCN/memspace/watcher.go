package memspace

import (
	"ComputeNode/msg"
	"fmt"
	"sync"
	"time"
)

// ChannelDescriptor 描述一个信道（Topic）的语义元数据和特征
// 这是实现“自适应路由”的核心结构
type ChannelDescriptor struct {
	chanId      uint64
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
	//id can be the same with the memspace
	id uint64
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
	// Key: Content  FeatureHash (or simple keyword) -> Value: Target Topic
	// todo 先跑通原型，具体设计后面再来
	routeCache map[string]string
	prefixKey  string
	dbClient   *NucleusClient
}

// ChannelInfo 用于序列化输出
type ChannelInfo struct {
	ChanId      uint64 `json:"chanId"`
	Topic       string `json:"topic"`
	Description string `json:"description"`
}

// NewWatcher creates a new Watcher instance
func NewWatcher(agentId uint64, dbClient *NucleusClient) *Watcher {
	watcher := &Watcher{
		id:               agentId,
		p2pChannels:      make(map[uint64]chan *msg.AgentMsg),
		topicSubscribers: make(map[string][]uint64),
		topicMetadata:    make(map[string]*ChannelDescriptor), // init
		routeCache:       make(map[string]string),
		prefixKey:        fmt.Sprintf("communication-%d", agentId),
		dbClient:         dbClient,
	}
	// 需要创建一个default topic
	watcher.registerTopic("Default", "Default topic of this watcher")
	return watcher
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
func (w *Watcher) subscribeTopic(agentID uint64, topic string, agentChannel chan *msg.AgentMsg) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.p2pChannels[agentID]; !ok {
		// 不存在就创建一个加入就好了
		w.p2pChannels[agentID] = agentChannel
	}

	subs := w.topicSubscribers[topic]
	for _, subID := range subs {
		if subID == agentID {
			return
		}
	}
	w.topicSubscribers[topic] = append(subs, agentID)
	return
}

func (w *Watcher) registerTopic(topic string, description string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.topicSubscribers[topic]; !exists {
		w.topicSubscribers[topic] = make([]uint64, 0)
		if _, exists := w.topicMetadata[topic]; exists {
			// 不一致的问题
			delete(w.topicMetadata, topic)
		}
		w.topicMetadata[topic] = &ChannelDescriptor{
			Topic:       topic,
			Description: description,
			Keywords:    make([]string, 0), // 初始为空，后续通过 Learn 填充
			LastActive:  time.Now(),
		}
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

// Send sends a message directly to a specific agent.
// 点对点发送：直接查 P2P 表。
// Send 重构版：Write Content -> Push Key
func (w *Watcher) Send(from uint64, to uint64, topic string, content string) error {
	if topic == "" {
		topic = "Default"
	}

	ts := time.Now().UnixNano()
	// Key: prefix/topic/ts/from
	key := fmt.Sprintf("%s/%s/%d/%d", w.prefixKey, topic, ts, from)

	// 1. 持久化 (只存 Content 字符串，或者简单的 JSON)
	// 这里我们直接存 content，简单粗暴

	if err := w.dbClient.DistributePut([]byte(key), []byte(content)); err != nil {
		return err
	}
	time.Sleep(3 * time.Second)

	//if err := w.dbClient.TxnPut([]byte(key), []byte(content)); err != nil {
	//	return err
	//}
	//err := w.dbClient.Commit()
	//if err != nil {
	//	return err
	//}
	// 2. 构造通知消息 (只包含 Key 和元数据，不包含巨大的 Content)
	// 我们复用 AgentMsg 结构，但 Content 字段放 Key，或者加一个 Key 字段
	// 为了不改结构体，我们暂且把 Key 放在 Msg 字段里，或者 Content 里
	notifyMsg := &msg.AgentMsg{
		From:  from,
		To:    to,
		Topic: topic,
		// 【关键】这里放 Key，而不是 Content！
		// 接收方看到这个，知道要去 DB 里拉数据
		Content: key,
		Ts:      ts,
		// 标记一下这是 Notification 还是 Full Payload？
		// 可以在 AgentMsg 加个 Type 字段，或者约定 Content 以 "key://" 开头
	}

	// 3. 内存推送
	w.mu.RLock()
	defer w.mu.RUnlock()

	if ch, ok := w.p2pChannels[to]; ok {
		select {
		case ch <- notifyMsg:
		default:
		}
	}
	return nil
}

// PublishTopic broadcasts a message to all agents subscribed to the topic.
func (w *Watcher) PublishTopic(from uint64, topic string) error {
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
func (w *Watcher) GenerateCommunicationMap() map[string][]*ChannelInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var channels map[string][]*ChannelInfo

	// 遍历 Topic Metadata
	for topic, desc := range w.topicMetadata {
		channelInfo := &ChannelInfo{
			ChanId:      desc.chanId,
			Topic:       topic,
			Description: desc.Description,
		}
		channels[topic] = append(channels[topic], channelInfo)
	}
	return channels
}
