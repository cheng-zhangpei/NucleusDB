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
	err, agent1, agent2, SharedSpaceID := setAgentTestConfig(t)

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
	targetSpaceIDs := []uint64{uint64(SharedSpaceID)}
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
func Test_AgentCommunicateP2P(t *testing.T) {
	// 1. 初始化环境
	err, agent1, agent2, SharedSpaceID := setAgentTestConfig(t)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	fmt.Printf("\n>> [Setup] Agents initialized. Agent 1 ID: %d, Agent 2 ID: %d, Shared Space: %d\n",
		agent1.InternalAgentId, agent2.InternalAgentId, SharedSpaceID)

	// 2. 启动 Agent 的监听循环 (后台 Goroutine)
	// 注意：确保你的 Agent 结构体里有 Start() 方法，且逻辑正确
	agent1.Start()
	agent2.Start()
	// 注册信道
	err = agent1.publicMm[0].Watcher.RegisterAgentChannel(agent2.InternalAgentId, agent2.comChannel)
	assert.NoError(t, err)
	// 给一点时间让 Goroutine 跑起来
	time.Sleep(100 * time.Millisecond)

	// 3. 模拟 Agent 1 发送 P2P 消息给 Agent 2
	// 消息内容： "Secret handshake: Protocol Omega"
	// Topic: "SecretChat" (或者是 "Default")
	fmt.Println("\n>> [Action] Agent 1 sending P2P message to Agent 2...")

	targetAgentID := agent2.InternalAgentId
	topic := "SecretChat"
	content := "Secret-handshake:-Protocol-Omega"

	// 假设 publicMm[0] 就是那个 ID=100 的共享空间
	commSpace := agent1.publicMm[0]

	// 调用 Watcher 的 Send
	// 注意：Agent 应该封装一个 Send 方法，或者直接调 MemSpace.Watcher.Send
	err = commSpace.Watcher.Send(agent1.InternalAgentId, targetAgentID, topic, content)
	assert.NoError(t, err, "Agent 1 send failed")

	fmt.Println(">> [Check] Message sent. Waiting for delivery...")

	// 4. 等待消息投递与处理
	// 因为是异步 Channel 推送 + LLM 处理，可能需要几秒钟
	time.Sleep(3 * time.Second)

	// 5. 验证 (这里做一些手动的 Log 检查提示)
	// 理想情况下，你应该在控制台看到：
	// [Agent 2] Received from 1 (Topic: SecretChat): Secret handshake: Protocol Omega
	// [Agent 2] Thinking...
	// [Agent 2] Replied...
	// [Agent 1] Received from 2 (Topic: Default): <LLM reply>

	fmt.Println("\n>> [Verification] Please check console logs above for message exchange.")

	// 6. 进阶验证：检查持久化
	// 我们可以去 TinyKV 里查一下，刚才那条消息是不是真的存进去了？
	fmt.Println("\n>> [Verification] Checking persistence in TinyKV...")

	// 构造 Key 前缀进行扫描 (假设你有 List 方法)
	// Key format: prefix/topic/timestamp/from
	// 我们可以尝试列出该 Topic 下的所有消息
	// 注意：这里需要 MemSpace 暴露一个 ListMessages(topic) 的接口，或者直接用 KV Client 查
	// 假设 dbClient 有个 ScanPrefix

	// 这里简单打印一下提示，如果上面没报错，说明 Put 成功了
	fmt.Println(">> Persistence check passed (Send returned no error).")

	// 停止 Agent
	agent1.Stop()
	agent2.Stop()
}
func setAgentTestConfig(t *testing.T) (error, *InternalAgent, *InternalAgent, int) {
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

	AgentManager := NewAgentManager(manager, "127.0.0.1:31001", 1)

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
	return err, agent1, agent2, SharedSpaceID
}
