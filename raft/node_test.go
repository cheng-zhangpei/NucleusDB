package raft

import (
	"testing"
)

// TEST NODE FUNCTIONS

func TestRaftFunction(t *testing.T) {
	config, err := LoadConfig("./configs/raft_config_1.yaml")
	if err != nil {
		panic(err)
	}
	StartNode(config)
}
