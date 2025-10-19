// Package ComputeNode

package ComputeNode

import (
	"ComputeNode/agent"
)

const NODETYPE = "VECTOR"
const META_PATH = "/nucleusDB/mempSpace/meta"

// ComputeNode : Unified External Interface for Computing Nodes
type ComputeNode struct {
	nodeType string
	nodeName string
	ap       *agent.AgentManager
}

func NewComputeNode(nodeType string, nodeName string, ap *agent.AgentManager) *ComputeNode {
	return &ComputeNode{}
}
