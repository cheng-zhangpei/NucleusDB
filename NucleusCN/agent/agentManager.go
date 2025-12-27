package agent

import (
	"ComputeNode/memspace"
	"fmt"
)

type AgentManager struct {
	internalAgents map[uint64]*InternalAgent
	mmManager      *memspace.MemSpaceManager
}

func NewAgentManager(memSpaceManager *memspace.MemSpaceManager) *AgentManager {
	return &AgentManager{
		internalAgents: make(map[uint64]*InternalAgent),
		mmManager:      memSpaceManager,
	}
}

// RegisterInternalAgent register a internal Agent(it is mean the agent style is defined by our system)
func (am *AgentManager) RegisterInternalAgent(id uint64, chatSeverAddr, embeddingServerAddr, character, work string) (*InternalAgent, error) {
	// 判断id是否重复
	if _, exists := am.internalAgents[id]; exists {
		return nil, fmt.Errorf("agent with id %d already exists", id)
	}
	agent := NewInternalAgent(id, chatSeverAddr, embeddingServerAddr, character, work, am.mmManager)
	am.internalAgents[id] = agent
	return agent, nil
}

// BindingPrivateMemSpace  binding memspace with agent
func (am *AgentManager) BindingPrivateMemSpace(agentId uint64, memSpaceId uint64) error {
	// 判断Agent是否已经被注册了 todo External Agent是否有必要呢？
	agent, exists := am.internalAgents[agentId]
	if !exists {
		return fmt.Errorf("agent with id %d does not exists, please register agent first", agentId)
	}
	// 判断当前Agent是否已经绑定了?
	if agent.isBindingPrivate != false {
		return fmt.Errorf("agent with id %d is already binding private memspace", agentId)
	}
	// 判断是否存在记忆空间如果不存在就创建
	if !am.mmManager.CanMountPrivate(memSpaceId) {
		return fmt.Errorf("can not bind private mem space %d", memSpaceId)
	}
	// 修改Agent信息
	agent.privateMemSpaceKeys = memSpaceId
	agent.isBindingPrivate = true

	// 修改对应卷信息
	space := am.mmManager.PrivateTable[memSpaceId]
	space.BindingAgents = append(space.BindingAgents, agentId)
	agent.privateMm = space
	return nil
}

// BindingPublicMemSpace  binding public memspace with agent
func (am *AgentManager) BindingPublicMemSpace(agentId uint64, memSpaceId uint64) error {
	if !am.mmManager.CanMountPublic(memSpaceId) {
		return fmt.Errorf("can not bind private mem space %d", memSpaceId)
	}
	agent, exists := am.internalAgents[agentId]
	if !exists {
		return fmt.Errorf("agent with id %d does not exists, please register agent first", agentId)
	}
	agent.sharedMemSpaceKeys = append(agent.sharedMemSpaceKeys, memSpaceId)
	space := am.mmManager.PublicTable[memSpaceId]
	space.BindingAgents = append(space.BindingAgents, agentId)
	agent.publicMm = append(agent.publicMm, space)
	return nil
}

// UnbindPrivateMemSpace 解绑Agent的私有记忆空间
func (am *AgentManager) UnbindPrivateMemSpace(agentId uint64, memId uint64) error {
	// 判断Agent是否存在
	agent, exists := am.internalAgents[agentId]
	if !exists {
		return fmt.Errorf("agent with id %d does not exist", agentId)
	}
	// 判断Agent是否已经绑定了私有记忆空间
	if !agent.isBindingPrivate {
		return fmt.Errorf("agent with id %d is not binding any private memspace", agentId)
	}
	// 检查要解绑的记忆空间ID是否匹配
	if agent.privateMemSpaceKeys != memId {
		return fmt.Errorf("agent with id %d is not binding private memspace %d", agentId, memId)
	}
	// 检查记忆空间是否存在
	space, exists := am.mmManager.PrivateTable[memId]
	if !exists {
		return fmt.Errorf("private memspace %d does not exist", memId)
	}
	// 从记忆空间的绑定Agent列表中移除
	space.BindingAgents = removeElement(space.BindingAgents, agentId)
	// 重置Agent的私有记忆空间信息
	agent.privateMemSpaceKeys = 0 // 或者你的未绑定状态值
	agent.isBindingPrivate = false

	return nil
}

// UnbindPublicMemSpace 解绑Agent的公共记忆空间
func (am *AgentManager) UnbindPublicMemSpace(agentId uint64, memId uint64) error {
	// 判断Agent是否存在
	agent, exists := am.internalAgents[agentId]
	if !exists {
		return fmt.Errorf("agent with id %d does not exist", agentId)
	}
	// 检查要解绑的记忆空间是否存在
	space, exists := am.mmManager.PublicTable[memId]
	if !exists {
		return fmt.Errorf("public memspace %d does not exist", memId)
	}
	// 检查Agent是否绑定了该公共记忆空间
	if !contains(agent.sharedMemSpaceKeys, memId) {
		return fmt.Errorf("agent with id %d is not binding public memspace %d", agentId, memId)
	}
	// 从Agent的共享记忆空间列表中移除
	agent.sharedMemSpaceKeys = removeElement(agent.sharedMemSpaceKeys, memId)
	// 从记忆空间的绑定Agent列表中移除
	space.BindingAgents = removeElement(space.BindingAgents, agentId)
	return nil
}

// 辅助函数：从uint64切片中移除指定元素
func removeElement(slice []uint64, elem uint64) []uint64 {
	result := make([]uint64, 0, len(slice))
	for _, item := range slice {
		if item != elem {
			result = append(result, item)
		}
	}
	return result
}

// 辅助函数：检查uint64切片是否包含指定元素
func contains(slice []uint64, elem uint64) bool {
	for _, item := range slice {
		if item == elem {
			return true
		}
	}
	return false
}

func (am *AgentManager) ListInternalAgent() []*InternalAgent {
	agents := make([]*InternalAgent, len(am.internalAgents))
	for _, agent := range am.internalAgents {
		agents = append(agents, agent)
	}
	return agents
}

func (am *AgentManager) PrintInternalAgent(id uint64) {
	for _, agent := range am.internalAgents {
		print(agent)
	}
}

func (am *AgentManager) GetInternalAgent(id uint64) (*InternalAgent, error) {
	agent, exist := am.internalAgents[id]
	if !exist {
		return nil, fmt.Errorf("agent with id %d does not exists", id)
	} else {
		return agent, nil
	}
}
