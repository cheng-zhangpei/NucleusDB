package raft

import (
	"testing"
)

// TEST NODE FUNCTIONS

// 测试是否会时间到期触发选举
// 计时器的设置是每一个节点都有一个对应的时钟触发器
func TestElectionTimeout(t *testing.T) {
	config1, err := LoadConfig("./configs/raft_config_1.yaml")
	if err != nil {
		panic(err)
	}
	config2, err := LoadConfig("./configs/raft_config_2.yaml")
	if err != nil {
		panic(err)
	}
	config3, err := LoadConfig("./configs/raft_config_3.yaml")
	if err != nil {
		panic(err)
	}
	// 开三个节点慢慢开始联测
	StartNode(config1)

	StartNode(config2)

	StartNode(config3)
	// 用一个无限循环 阻塞测试进程，
	// 否则在子线程调度的时候会将主进程退出从而无法进行测试

	for {

	}
}
