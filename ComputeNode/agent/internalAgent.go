package agent

import (
	"ComputeNode/agent/call"
	"ComputeNode/memspace"
)

type InternalAgent struct {
	InternalAgentId     uint64
	privateMemSpaceKeys uint64
	sharedMemSpaceKeys  []uint64
	privateMm           *memspace.MemSpace

	embeddingServer  *memspace.EmbeddingServerClient
	chatClient       *call.ChatServerClient
	isBindingPrivate bool
}

func NewInternalAgent(InternalAgentId uint64, chatServerHttpAddr string, embeddingServerAddr string) *InternalAgent {
	embeddingServerClient := memspace.NewEmbeddingServerClient(embeddingServerAddr)
	chatClient := call.NewChatServerClient(chatServerHttpAddr)

	return &InternalAgent{
		sharedMemSpaceKeys: make([]uint64, 0),
		InternalAgentId:    InternalAgentId,
		embeddingServer:    embeddingServerClient,
		chatClient:         chatClient,
		isBindingPrivate:   false,
	}
}

// todo 更多类型的InternalAgent支持

// -----------------------------------define the action of InternalAgent---------------------------------------------

// TempOutput output base only for temporary memory space
func (ag *InternalAgent) TempOutput() {

}

// CompositeOutput Organize all memory spaces and output the results.
func (ag *InternalAgent) CompositeOutput() {

}

// SpecifyOutput The InternalAgent can specify which memory spaces to use for output.
func (ag *InternalAgent) SpecifyOutput(specifyMMKey []string) string {
	return ""
}

// SendMessage2LLMServer Send the prompt to the large model service
func (ag *InternalAgent) SendMessage2LLMServer() {

}

// GetMyMemorySpaceAbstract call by InternalAgent
func (ag *InternalAgent) GetMyMemorySpaceAbstract() {

}
