package memspace

import "strings"

// removeSpecialCharacters 移除特殊字符，保留字母、数字、中文和基本标点
func removeSpecialCharacters(text string) string {
	var result strings.Builder
	for _, r := range text {
		// 保留：字母、数字、空格、中文
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r == ' ') ||
			(r >= '\u4e00' && r <= '\u9fff') { // 中文范围
			result.WriteRune(r)
		}
	}
	return result.String()
}

// cleanForToolMemory 工具记忆的特定清理
func cleanForToolMemory(text string) string {
	var result strings.Builder
	for _, r := range text {
		// 保留字母、数字、空格
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r == ' ') {
			result.WriteRune(r)
		} else if r == ',' || r == '(' || r == ')' {
			// 对于逗号和括号，添加空格来分隔单词
			result.WriteRune(' ')
		}
	}

	// 清理多余的空格
	return strings.Join(strings.Fields(result.String()), " ")
}

// cleanForContentMemory 内容记忆的特定清理
func cleanForContentMemory(text string) string {
	var result strings.Builder
	for _, r := range text {
		// 只保留字母、数字、空格、中文
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r == ' ') ||
			(r >= '\u4e00' && r <= '\u9fff') {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// cleanForBehavioralMemory 行为记忆的特定清理
func cleanForBehavioralMemory(text string) string {
	// 行为记忆保留字母、数字、中文
	var result strings.Builder
	for _, r := range text {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r == ' ') ||
			(r >= '\u4e00' && r <= '\u9fff') {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// cleanForEpisodicMemory 情景记忆的特定清理
func cleanForEpisodicMemory(text string) string {
	// 情景记忆保留字母、数字、中文
	var result strings.Builder
	for _, r := range text {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			(r == ' ') ||
			(r >= '\u4e00' && r <= '\u9fff') {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// removeStopWords 移除停用词
func removeStopWords(text string) string {
	stopWords := []string{"的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都", "一", "一个", "上", "也", "很", "到", "说", "要", "去", "你", "会", "着", "没有", "看", "好", "自己", "这", "那", "但", "什么", "把", "又", "可以"}

	words := strings.Fields(text)
	var result []string

	for _, word := range words {
		isStopWord := false
		for _, stopWord := range stopWords {
			if word == stopWord {
				isStopWord = true
				break
			}
		}
		if !isStopWord && len(word) > 0 {
			result = append(result, word)
		}
	}

	return strings.Join(result, " ")
}
