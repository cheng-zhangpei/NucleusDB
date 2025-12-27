package agent

import (
	"ComputeNode/agent/call"
	"ComputeNode/memspace"
	"ComputeNode/msg"
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
)

type InternalAgent struct {
	InternalAgentId     uint64
	privateMemSpaceKeys uint64
	sharedMemSpaceKeys  []uint64
	privateMm           *memspace.MemSpace
	publicMm            []*memspace.MemSpace
	embeddingServer     *memspace.EmbeddingServerClient
	chatClient          *call.ChatServerClient
	isBindingPrivate    bool

	character    string // the character of the agent
	work         string // introduce the detail of it`s job
	systemPrompt string

	msgBuffer []*msg.AgentMsg
	mmManager *memspace.MemSpaceManager
}

func NewInternalAgent(InternalAgentId uint64, chatServerHttpAddr,
	embeddingServerAddr, character, work string, mmManager *memspace.MemSpaceManager) *InternalAgent {
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
		mmManager:          mmManager,
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
// 自动整合所有绑定的记忆空间进行检索，并根据 LLM 的决策进行路由分发。
func (ag *InternalAgent) CompositeOutput(input string) (string, error) {
	if !ag.isBindingPrivate {
		return "", fmt.Errorf("private memory space is not bound")
	}

	// 1. collect all memspace todo the authority check operation
	var targetSpaces []*memspace.MemSpace
	targetSpaces = append(targetSpaces, ag.privateMm) // 私有
	if len(ag.publicMm) > 0 {
		targetSpaces = append(targetSpaces, ag.publicMm...) // 公共
	}

	// 2. 执行 RAG + 路由决策流程 (使用新的 executeRouterChat)
	agentResp, err := ag.executeRAGChat(targetSpaces, input)
	if err != nil {
		return "", err
	}
	parsedAgentResp := parseLLMResponse(agentResp)
	// parsedAgentResp: {Content, TargetTopic, TargetAgent}
	// 3.  Self-Reflection
	memRecord := fmt.Sprintf("User Input: %s | My Response: %s | Routing: Topic=%s, Agent=%s",
		input, parsedAgentResp.Content, parsedAgentResp.TargetTopic, parsedAgentResp.TargetAgent)

	if err := ag.privateMm.SaveTempMemory(memRecord, ag.InternalAgentId); err != nil {
		log.Printf("Failed to save private temp memory: %v", err)
	}

	// 4. 执行路由动作 (Act on Routing Decision)
	// 这一步是将 LLM 的决策转化为实际的系统行为
	//if err := ag.dispatchMessage(agentResp); err != nil {
	//	log.Printf("Routing dispatch warning: %v", err)
	//	// 注意：路由失败不应该导致整个函数返回错误，因为回答内容已经生成了
	//}
	return parsedAgentResp.Content, nil
}

// SpecifyOutput The InternalAgent can specify which memory spaces to use for output.
// 指定特定的记忆空间 ID 进行检索和回答
func (ag *InternalAgent) SpecifyOutput(specifyMMKey []uint64, input string) (string, error) {
	if !ag.isBindingPrivate {
		return "", fmt.Errorf("private memory space is not bound")
	}

	// 1. 筛选并验证请求的空间
	var targetSpaces []*memspace.MemSpace

	for _, key := range specifyMMKey {
		// 检查是否是私有空间
		if key == ag.privateMm.MemSpaceId {
			targetSpaces = append(targetSpaces, ag.privateMm)
			continue
		}

		// 检查是否在已绑定的公共空间列表中
		found := false
		for _, pubSpace := range ag.publicMm {
			if pubSpace.MemSpaceId == key {
				// 双重检查权限（调用 Manager）
				if ag.mmManager.CheckAuthority(ag.InternalAgentId, key) {
					targetSpaces = append(targetSpaces, pubSpace)
					found = true
				} else {
					log.Printf("[Agent %d] Access denied for Space %d", ag.InternalAgentId, key)
				}
				break
			}
		}

		if !found {
			log.Printf("[Agent %d] Warning: Requested space %d is not bound or not found", ag.InternalAgentId, key)
		}
	}

	if len(targetSpaces) == 0 {
		return "", fmt.Errorf("no valid memory spaces selected for output")
	}

	// 2. 执行 RAG 流程
	return ag.executeRAGChat(targetSpaces, input)
}

func (ag *InternalAgent) getPrivateSpaceMemory() {

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

// MountSpace Mounting here refers to the ability to directly use metadata from the memSpace as agent
// attributes without going through the MemManager, enabling direct access to content within the data space.
func (ag *InternalAgent) MountSpace(publicMmKey uint64) error {
	// 1. check if the memSpace can be mounted
	if !ag.mmManager.CanMountPublic(publicMmKey) {
		return fmt.Errorf("can not mount public memory : %d", publicMmKey)
	}

	return nil
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

// CompositeInputBuilder 使用新的 RAGTemplate 填充数据
func (ag *InternalAgent) CompositeInputBuilder(context, tempMem, input string) (string, error) {
	var buf bytes.Buffer

	// 使用新的结构体 RAGPromptData
	data := RouterRAGData{
		Context:    context,
		TempMemory: tempMem,
		Input:      input,
	}

	// 使用新的模板对象 RAGTemplate
	if err := RouterRAGTemplate.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("RAG template execute failed: %w", err)
	}
	return buf.String(), nil
}

// ------------------------------------------------------------------------------------------------
//                                   RAG
// ------------------------------------------------------------------------------------------------

// executeRAGChat 包含核心逻辑：向量搜索 -> 构建 Prompt -> 调用大模型 -> 更新短期记忆
func (ag *InternalAgent) executeRAGChat(spaces []*memspace.MemSpace, input string) (string, error) {
	// 1. 检索语义上下文 (Vector Search)
	// 我们从每个选定的空间中检索 TopK 个最相似的记录
	var contextBuilder strings.Builder
	const TopK = 3

	foundAnyContext := false

	for _, space := range spaces {
		// 调用 MemSpace 的 SemanticSearch 方法
		records, err := space.SemanticSearch(input, TopK)
		if err != nil {
			log.Printf("Error searching space %d: %v", space.MemSpaceId, err)
			continue
		}

		if len(records) > 0 {
			foundAnyContext = true
			contextBuilder.WriteString(fmt.Sprintf("--- Knowledge from Space [ID:%d] ---\n", space.MemSpaceId))
			for _, rec := range records {
				if rec.Content != "" {
					contextBuilder.WriteString(fmt.Sprintf("- %s\n", rec.Content))
				}
			}
			contextBuilder.WriteString("\n")
		}
	}

	// 如果没有找到任何上下文，可以给个默认提示，或者直接留空
	retrievedContext := contextBuilder.String()
	if !foundAnyContext {
		retrievedContext = "No relevant long-term memory found."
	}

	// 2. 获取短期记忆 (Short-term context)
	// 通常我们总是从私有空间获取当前的对话流上下文
	tempHistory := ag.privateMm.GetTempSpaceMemory()

	// 3. 构建最终 Prompt (使用重命名后的 buildRAGPrompt)
	finalPrompt, err := ag.CompositeInputBuilder(retrievedContext, tempHistory, input)
	if err != nil {
		return "", err
	}

	// 4. 调用 LLM
	// System Prompt 依然使用 agent 初始化时设定的角色设定
	response, err := ag.chatClient.QuickChat(finalPrompt, ag.systemPrompt)
	if err != nil {
		return "", fmt.Errorf("LLM chat failed: %w", err)
	}

	// 5. 将本次交互保存回私有短期记忆
	// 保存格式：用户输入 + 模型回答
	interactionRecord := fmt.Sprintf("User: %s | Agent: %s", input, response.Response)
	if err := ag.privateMm.SaveTempMemory(interactionRecord, ag.InternalAgentId); err != nil {
		log.Printf("Failed to save temp memory: %v", err)
	}
	resp := response.Response

	return resp, nil
}
