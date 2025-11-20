package agent

import (
	"text/template"
)

// 默认模板：把临时记忆放在前面，用户 input 放在后面，中间用空行隔开。
// 你可以换成任意 prompt，比如“逐条回忆”风格、JSON 风格、XML 风格等。
const defaultPromptTmpl = `Below is the temporary memory:
{{.TempMemory}}
User Input:
{{.Input}}`

type promptTmplData struct {
	TempMemory string
	Input      string
}

// 允许外部注入模板，便于不同场景复用同一段代码
var promptTemplate = template.Must(template.New("tempMemoPrompt").Parse(defaultPromptTmpl))
