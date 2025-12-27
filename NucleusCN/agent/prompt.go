package agent

import (
	"text/template"
)

// ======================================================================================
// 1. 基础模板 (Basic Prompt)
// 用于 TempChat，仅依赖短期记忆，不需要复杂的路由决策
// ======================================================================================

const defaultPromptTmpl = `Below is the temporary memory:
{{.TempMemory}}
User Input:
{{.Input}}`

type promptTmplData struct {
	TempMemory string
	Input      string
}

var promptTemplate = template.Must(template.New("tempMemoPrompt").Parse(defaultPromptTmpl))

// ======================================================================================
// 2. RAG 路由模板 (Router RAG Prompt) - 核心修改部分
// 用于 CompositeOutput/SpecifyOutput，支持 RAG 检索 + 动态路由决策 + JSON 输出
// ======================================================================================

const RouterRAGPromptContent = `
[Role Definition]:
You are an intelligent agent in a distributed system. 
Your goal is to answer the user's input based on retrieved memories and decide which communication channel to use for your response.

[Communication Network (Routing Table)]:
The following channels are available. Choose the one that best fits your response content:
{{.CommMap}}

[System Context / Retrieved Long-term Memories]:
{{.Context}}

[Recent Short-term Conversation]:
{{.TempMemory}}

[User Input]:
{{.Input}}

[Output Requirement]:
You MUST respond in strict JSON format. Do not output any markdown code blocks (like ` + "`" + `json...` + "`" + `) or extra text.
The JSON structure must be:
{
  "content": "Your actual response content goes here.",
  "target_topic": "Choose a topic from the Communication Network above. If no specific topic fits, use 'general'."
  "target_agent": "Choose a agent from the Communication Network above. you can just choose one agent,if you do not want to choose,use 'nil',it do not affect you choose the topic."
}
`

// RouterRAGData 用于填充路由模板的数据结构
type RouterRAGData struct {
	CommMap    string // 通讯录 (Watcher.GenerateCommunicationMap 的结果)
	Context    string // 长期记忆 (RAG 检索结果)
	TempMemory string // 短期记忆
	Input      string // 用户输入
}

// RouterRAGTemplate 全局单例模板对象
var RouterRAGTemplate = template.Must(template.New("routerRAGPrompt").Parse(RouterRAGPromptContent))
