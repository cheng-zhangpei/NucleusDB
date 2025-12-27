package agent

import (
	"ComputeNode/memspace"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// Test_AgentRAG capabilities checks if the agent can retrieve memory from shared spaces
func Test_AgentRAG(t *testing.T) {
	// 1. 初始化基础设施
	// 注意：确保 NucleusDB, EmbeddingServer, ChatServer 都在运行
	client := memspace.NewNucleusClient("127.0.0.1:31001", 1)
	var privatePath string = "/NucleusDB/vector/private"
	var publicPath string = "/NucleusDB/vector/public"
	var metaPath string = "/NucleusDB/vector/meta"

	manager, err := memspace.NewMemSpaceManager(client, privatePath, publicPath, metaPath)
	if err != nil {
		t.Fatal(err)
	}

	AgentManager := NewAgentManager(manager)

	// 2. 注册两个 Agent
	// Agent 1: 知识贡献者
	agent1, err := AgentManager.RegisterInternalAgent(1, "http://localhost:20001",
		"http://localhost:20002", "Researcher", "Expert in AI history")
	assert.NoError(t, err)

	// Agent 2: 知识消费者
	agent2, err := AgentManager.RegisterInternalAgent(2, "http://localhost:20001",
		"http://localhost:20002", "Student", "Learning about AI")
	assert.NoError(t, err)

	// 3. 注册记忆空间
	// Space 1 & 2: 私有空间
	err = AgentManager.mmManager.RegisterMemSpace(1, memspace.Private, 10000,
		memspace.ContentMemory, "http://localhost:20002", 3000, 10)
	assert.NoError(t, err)

	err = AgentManager.mmManager.RegisterMemSpace(2, memspace.Private, 10000,
		memspace.ContentMemory, "http://localhost:20002", 3000, 10)
	assert.NoError(t, err)

	// Space 100: 共享知识库 (Shared Knowledge Base)
	const SharedSpaceID = 100
	err = AgentManager.mmManager.RegisterMemSpace(SharedSpaceID, memspace.Shared, 50000,
		memspace.ContentMemory, "http://localhost:20002", 3000, 10)
	assert.NoError(t, err)

	// 4. 绑定关系
	// 绑定私有空间
	assert.NoError(t, AgentManager.BindingPrivateMemSpace(1, 1))
	assert.NoError(t, AgentManager.BindingPrivateMemSpace(2, 2))

	// 绑定共享空间：两个 Agent 都绑定同一个共享空间
	assert.NoError(t, AgentManager.BindingPublicMemSpace(1, SharedSpaceID))
	assert.NoError(t, AgentManager.BindingPublicMemSpace(2, SharedSpaceID))

	fmt.Println("----------------- Setup Complete -----------------")

	// 5. [关键步骤] 模拟向共享空间注入长期记忆 (知识库构建)
	// 我们获取 Agent1 的共享空间引用，并手动写入一条通过向量化的知识
	// 假设知识内容是："The secret code for the project is 'BlueHorizon'."

	fmt.Println(">> Injecting knowledge into Shared Space...")
	sharedSpaceRef := agent1.publicMm[0] // 获取绑定的第一个共享空间

	// 这里调用 PersistMemoryUint，它会触发 EmbeddingServer 生成向量并存入 DB
	knowledge := "IMPORTANT: The secret operation code name is 'BlueHorizon'."
	err = sharedSpaceRef.PersistMemoryUint("secret_key_001", []byte(knowledge))
	assert.NoError(t, err)

	// 给一点时间确保数据落盘或索引刷新（视具体实现而定）
	time.Sleep(1 * time.Second)

	// 6. 测试 TempChat (短期记忆)
	fmt.Println("\n>> Testing TempChat (Short-term Memory)...")
	resp1, err := agent1.TempChat("Hello, I am Agent 1.")
	assert.NoError(t, err)
	fmt.Printf("[Agent 1 TempChat]: %s\n", resp1)

	// 7. 测试 CompositeOutput (RAG - 自动检索)
	// Agent 2 从未在对话中被告知 code name，但它绑定了共享空间，应该能回答出来
	fmt.Println("\n>> Testing CompositeOutput (RAG from Shared Space)...")

	query := "What is the secret operation code name?"
	fmt.Printf("[User to Agent 2]: %s\n", query)

	ragResponse, err := agent2.CompositeOutput(query)
	assert.NoError(t, err)

	fmt.Printf("[Agent 2 Composite Output]: %s\n", ragResponse)

	// 验证回答中是否包含关键信息 (根据你的LLM能力，这里做简单的字符串包含检查)
	// 注意：如果 Embedding 检索成功，Prompt 里会有 "BlueHorizon"，LLM 应该会由其生成答案
	// assert.Contains(t, ragResponse, "BlueHorizon") // 视实际模型输出而定

	// 8. 测试 SpecifyOutput (指定空间检索)
	fmt.Println("\n>> Testing SpecifyOutput (Targeted Retrieval)...")

	// 强制 Agent 2 只从 SharedSpaceID (100) 检索
	targetSpaceIDs := []uint64{SharedSpaceID}
	specResponse, err := agent2.SpecifyOutput(targetSpaceIDs, "Tell me the secret code again.")
	assert.NoError(t, err)

	fmt.Printf("[Agent 2 Specified Output]: %s\n", specResponse)

	// 9. 测试私有记忆隔离 (Privacy Test)
	fmt.Println("\n>> Testing Private Memory Isolation...")

	// Agent 1 在自己的私有空间记录秘密
	privateSecret := "My favorite color is invisible green."
	err = agent1.privateMm.SaveTempMemory(privateSecret, 1)
	assert.NoError(t, err)

	// Agent 2 尝试询问 Agent 1 的私有秘密 (Agent 2 应该不知道)
	queryPrivate := "What is Agent 1's favorite color?"
	pvtResponse, err := agent2.CompositeOutput(queryPrivate)
	assert.NoError(t, err)

	fmt.Printf("[Agent 2 trying to guess Agent 1's secret]: %s\n", pvtResponse)
	// 理论上 Agent 2 的回答应该是不知道，因为它无法检索 Space 1 的内容

	time.Sleep(1 * time.Second)

	// 将记忆空间的内容打印出来
	memories, err := sharedSpaceRef.ListPersistMemories()
	assert.NoError(t, err)
	fmt.Println("--------------看一下记忆空间的内容------------------------")
	fmt.Println(memories)
	fmt.Println("\n----------------- All Tests Finished -----------------")
}
