package call

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ChatServerClient struct {
	httpServerAddr string
	httpClient     *http.Client
}

// NewChatServerClient 创建新的聊天服务器客户端
func NewChatServerClient(serverAddr string) *ChatServerClient {
	return &ChatServerClient{
		httpServerAddr: serverAddr,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// HealthResponse 健康检查响应
type HealthResponse struct {
	Status            string  `json:"status"`
	Model             string  `json:"model"`
	Timestamp         float64 `json:"timestamp"`
	ClientInitialized bool    `json:"client_initialized"`
}

// ChatMessage 聊天消息
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatCompletionRequest 聊天补全请求
type ChatCompletionRequest struct {
	Messages    []ChatMessage `json:"messages"`
	Stream      bool          `json:"stream,omitempty"`
	Temperature float64       `json:"temperature,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
}

// ChatCompletionResponse 聊天补全响应
type ChatCompletionResponse struct {
	Choices []struct {
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
	Model     string  `json:"model"`
	Timestamp float64 `json:"timestamp"`
}

// QuickChatRequest 快速聊天请求
type QuickChatRequest struct {
	Message      string `json:"message"`
	SystemPrompt string `json:"system_prompt,omitempty"`
}

// QuickChatResponse 快速聊天响应
type QuickChatResponse struct {
	Response  string  `json:"response"`
	Usage     any     `json:"usage"` // 使用 interface{} 因为可能是 null 或对象
	Timestamp float64 `json:"timestamp"`
}

// ConversationRequest 多轮对话请求
type ConversationRequest struct {
	Conversation []ChatMessage `json:"conversation,omitempty"`
	NewMessage   string        `json:"new_message,omitempty"`
}

// ConversationResponse 多轮对话响应
type ConversationResponse struct {
	Response            string        `json:"response"`
	UpdatedConversation []ChatMessage `json:"updated_conversation"`
	Usage               any           `json:"usage"`
	Timestamp           float64       `json:"timestamp"`
}

// ModelsResponse 模型列表响应
type ModelsResponse struct {
	CurrentModel    string            `json:"current_model"`
	SupportedModels []string          `json:"supported_models"`
	Parameters      map[string]string `json:"parameters"`
	MessageFormat   map[string]string `json:"message_format"`
}

// HealthCheck 健康检查
func (c *ChatServerClient) HealthCheck() (*HealthResponse, error) {
	resp, err := c.httpClient.Get(c.httpServerAddr + "/health")
	if err != nil {
		return nil, fmt.Errorf("health check request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}

	var healthResp HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&healthResp); err != nil {
		return nil, fmt.Errorf("failed to decode health response: %v", err)
	}

	return &healthResp, nil
}

// ChatCompletion 聊天补全
func (c *ChatServerClient) ChatCompletion(req ChatCompletionRequest) (*ChatCompletionResponse, error) {
	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	resp, err := c.httpClient.Post(c.httpServerAddr+"/chat/completions", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("chat completion request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("chat completion failed with status %d: %s", resp.StatusCode, string(body))
	}

	var chatResp ChatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return nil, fmt.Errorf("failed to decode chat response: %v", err)
	}

	return &chatResp, nil
}

// StreamChatCompletion 流式聊天补全
func (c *ChatServerClient) StreamChatCompletion(req ChatCompletionRequest) (io.ReadCloser, error) {
	req.Stream = true
	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	resp, err := c.httpClient.Post(c.httpServerAddr+"/chat/completions", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("stream chat completion request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("stream chat completion failed with status %d: %s", resp.StatusCode, string(body))
	}

	return resp.Body, nil
}

// QuickChat 快速聊天
func (c *ChatServerClient) QuickChat(message string, systemPrompt ...string) (*QuickChatResponse, error) {
	req := QuickChatRequest{
		Message: message,
	}

	if len(systemPrompt) > 0 {
		req.SystemPrompt = systemPrompt[0]
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal quick chat request: %v", err)
	}

	resp, err := c.httpClient.Post(c.httpServerAddr+"/quick-chat", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("quick chat request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("quick chat failed with status %d: %s", resp.StatusCode, string(body))
	}

	var quickResp QuickChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&quickResp); err != nil {
		return nil, fmt.Errorf("failed to decode quick chat response: %v", err)
	}

	return &quickResp, nil
}

// MultiTurnChat 多轮对话
func (c *ChatServerClient) MultiTurnChat(conversation []ChatMessage, newMessage string) (*ConversationResponse, error) {
	req := ConversationRequest{
		Conversation: conversation,
		NewMessage:   newMessage,
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal conversation request: %v", err)
	}

	resp, err := c.httpClient.Post(c.httpServerAddr+"/conversation", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("conversation request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("conversation failed with status %d: %s", resp.StatusCode, string(body))
	}

	var convResp ConversationResponse
	if err := json.NewDecoder(resp.Body).Decode(&convResp); err != nil {
		return nil, fmt.Errorf("failed to decode conversation response: %v", err)
	}

	return &convResp, nil
}

// ListModels 获取模型列表
func (c *ChatServerClient) ListModels() (*ModelsResponse, error) {
	resp, err := c.httpClient.Get(c.httpServerAddr + "/models")
	if err != nil {
		return nil, fmt.Errorf("models request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("models request failed with status: %d", resp.StatusCode)
	}

	var modelsResp ModelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&modelsResp); err != nil {
		return nil, fmt.Errorf("failed to decode models response: %v", err)
	}

	return &modelsResp, nil
}

// 使用示例
func main() {
	// 创建客户端
	client := NewChatServerClient("http://localhost:5000")

	// 健康检查
	health, err := client.HealthCheck()
	if err != nil {
		fmt.Printf("Health check failed: %v\n", err)
		return
	}
	fmt.Printf("Service status: %s, Model: %s\n", health.Status, health.Model)

	// 快速聊天示例
	quickResp, err := client.QuickChat("你好，请介绍一下你自己")
	if err != nil {
		fmt.Printf("Quick chat failed: %v\n", err)
		return
	}
	fmt.Printf("Quick chat response: %s\n", quickResp.Response)

	// 完整聊天示例
	chatReq := ChatCompletionRequest{
		Messages: []ChatMessage{
			{Role: "system", Content: "你是一个有用的助手"},
			{Role: "user", Content: "请用Go语言写一个Hello World程序"},
		},
		Temperature: 0.7,
		MaxTokens:   1024,
	}

	chatResp, err := client.ChatCompletion(chatReq)
	if err != nil {
		fmt.Printf("Chat completion failed: %v\n", err)
		return
	}

	if len(chatResp.Choices) > 0 {
		fmt.Printf("Assistant: %s\n", chatResp.Choices[0].Message.Content)
	}

	// 多轮对话示例
	conversation := []ChatMessage{
		{Role: "user", Content: "我喜欢编程"},
		{Role: "assistant", Content: "那很棒！编程是很有用的技能。"},
	}

	convResp, err := client.MultiTurnChat(conversation, "你能给我一些学习建议吗？")
	if err != nil {
		fmt.Printf("Multi-turn chat failed: %v\n", err)
		return
	}
	fmt.Printf("Multi-turn response: %s\n", convResp.Response)

	// 获取模型列表
	models, err := client.ListModels()
	if err != nil {
		fmt.Printf("List models failed: %v\n", err)
		return
	}
	fmt.Printf("Current model: %s, Supported: %v\n", models.CurrentModel, models.SupportedModels)
}
