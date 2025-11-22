package agent

import (
	"ComputeNode/agent/call"
	"ComputeNode/memspace"
	"bytes"
	"errors"
	"fmt"
)

type InternalAgent struct {
	InternalAgentId     uint64
	privateMemSpaceKeys uint64
	sharedMemSpaceKeys  []uint64
	privateMm           *memspace.MemSpace

	embeddingServer  *memspace.EmbeddingServerClient
	chatClient       *call.ChatServerClient
	isBindingPrivate bool

	character    string // the character of the agent
	work         string // introduce the detail of it`s job
	systemPrompt string
}

func NewInternalAgent(InternalAgentId uint64, chatServerHttpAddr,
	embeddingServerAddr, character, work string) *InternalAgent {
	embeddingServerClient := memspace.NewEmbeddingServerClient(embeddingServerAddr)
	chatClient := call.NewChatServerClient(chatServerHttpAddr)

	return &InternalAgent{
		sharedMemSpaceKeys: make([]uint64, 0),
		InternalAgentId:    InternalAgentId,
		embeddingServer:    embeddingServerClient,
		chatClient:         chatClient,
		isBindingPrivate:   false,
		character:          character,
		work:               work,
		systemPrompt:       fmt.Sprintf("[role%s,Work:%s]", character, work),
	}
}

// -----------------------------------define the action of InternalAgent---------------------------------------------

// TempChat output base only for temporary memory space
func (ag *InternalAgent) TempChat(input string) (string, error) {
	// if private space bind?
	if !ag.isBindingPrivate {
		return "", fmt.Errorf("the private memory space is not bind")
	}
	privateMemSpace := ag.privateMm
	tempMemory := privateMemSpace.GetTempSpaceMemory()
	// combine the temp memory and the input
	combineInput, err := ag.TempInputBuilder(tempMemory, input)
	if err != nil {
		return "", err
	}
	// call the llm response
	response, err := ag.chatClient.QuickChat(combineInput, ag.systemPrompt)
	if err != nil {
		return "", err
	}
	// constitute the MemRecord and save it into the memspace
	record := fmt.Sprintf("user/input:%s, modelOutput:%s", combineInput, response.Response)
	err = privateMemSpace.SaveTempMemory(record, ag.InternalAgentId)
	if err != nil {
		return "", err
	}
	return response.Response, nil
}

// CompositeOutput Organize all memory spaces and output the results.
func (ag *InternalAgent) CompositeOutput(input string) {

}

// SpecifyOutput The InternalAgent can specify which memory spaces to use for output.
func (ag *InternalAgent) SpecifyOutput(specifyMMKey []string, input string) string {
	return ""
}

// SendMessage2LLMServer Send the prompt to the large model service
func (ag *InternalAgent) SendMessage2LLMServer() {

}

// GetMyMemorySpaceAbstract call by InternalAgent
func (ag *InternalAgent) GetMyMemorySpaceAbstract() {

}

// savePrivateTempMemory save memory into memory space, the agent can only save memory into the temp partition of the memory space
func (ag *InternalAgent) savePrivateTempMemory(content string) {

}

func (ag *InternalAgent) saveSharedTempMemory(sharedMemKey uint64) {

}

//---------------------------------------------------------------Input Builder--------------------------------------

// TempInputBuilder combine the temp memory with  input
func (ag *InternalAgent) TempInputBuilder(tempInput, input string) (string, error) {
	if !ag.isBindingPrivate {
		return "", errors.New("private memory space not bound")
	}

	var buf bytes.Buffer
	if err := promptTemplate.Execute(&buf, promptTmplData{
		TempMemory: tempInput,
		Input:      input,
	}); err != nil {
		return "", fmt.Errorf("template execute: %w", err)
	}
	return buf.String(), nil
}
