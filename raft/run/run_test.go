package main

import (
	"os"
	"testing"
)

// 测一下配置文件以 及 节点是否正常运行
func TestRun(t *testing.T) {
	os.Setenv("NODE_ID", "3") // 手动设置环境变量
	main()
}
