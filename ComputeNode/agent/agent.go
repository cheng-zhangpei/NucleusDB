package agent

import "ComputeNode/memspace"

type Agent struct {
	agentId uint64
	// todo 暂时只是支持用http的方式加载大模型
	httpAddress         string
	privateMemSpaceKeys []string
	sharedMemSpaceKeys  []string
	mmManager           *memspace.MemSpaceManager
	// todo 目前的思维是，每一个记忆空间必须有一个独属的会话空间
	privateMm *memspace.MemSpace
}

func NewAgent(agentId uint64, httpAddr string, mmManager *memspace.MemSpaceManager) *Agent {
	return &Agent{
		mmManager:   mmManager,
		agentId:     agentId,
		httpAddress: httpAddr,
	}
}

// todo 更多类型的agent支持

// -----------------------------------define the action of agent---------------------------------------------

// TempOutput output base only for temporary memory space
func (ag *Agent) TempOutput() {

}

// CompositeOutput Organize all memory spaces and output the results.
func (ag *Agent) CompositeOutput() {

}

// SpecifyOutput The agent can specify which memory spaces to use for output.
func (ag *Agent) SpecifyOutput(specifyMMKey []string) string {
	return ""
}

// SendMessage2LLMServer Send the prompt to the large model service
func (ag *Agent) SendMessage2LLMServer() {

}

// GetMyMemorySpaceAbstract call by agent
func (ag *Agent) GetMyMemorySpaceAbstract() {

}
