package main

import (
	"os"
	"testing"
)

// 测一下配置文件以 及 节点是否正常运行
func TestRun(t *testing.T) {
	os.Setenv("NODE_ID", "3") // 手动设置环境变量

}

func TestStartNode1(t *testing.T) {
	os.Setenv("NODE_ID", "1") // 手动设置环境变量
	startNode("./data1", "../configs/raft_config_1.yaml")
}

func TestStartNode2(t *testing.T) {
	os.Setenv("NODE_ID", "2") // 手动设置环境变量
	startNode("./data2", "../configs/raft_config_2.yaml")

}

func TestStartNode3(t *testing.T) {
	os.Setenv("NODE_ID", "3") // 手动设置环境变量
	startNode("./data3", "../configs/raft_config_3.yaml")

}
