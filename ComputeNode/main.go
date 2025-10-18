// Package ComputeNode

package ComputeNode

import (
	"ComputeNode/agent"
	"ComputeNode/memspace"
)

const NODETYPE = "VECTOR"

// ComputeNode : Unified External Interface for Computing Nodes
type ComputeNode struct {
	nodeType string
	nodeName string
	mpm      *memspace.MemSpaceManager
	ap       *agent.AgentPool
}
