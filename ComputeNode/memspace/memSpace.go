package memspace

import (
	"ComputeNode/code"
	"ComputeNode/compute"
)

// MemSpaceType the type pf the memspace
type MemSpaceType int

const (
	Private MemSpaceType = iota
	Shared
)

// MemSpaceStatus the status of the memsapce
type MemSpaceStatus int

const (
	Pending MemSpaceStatus = iota
	Binding
	Corrupt
	Writing // there have another agent update the space
)

type MemSpace struct {
	// allow multi-agent binding
	bindingAgents []uint64
	// memory uint layout// todo 思考一下这里的Uint也就是实际的记忆空间到底如何搞比较好
	memUints []*MemUint
	// the type of
	spaceType *MemSpaceType
	// status
	spaceStatus *MemSpaceStatus
	// decode data todo：how to adapt multi-data type
	decodeData *code.VectorRecord
	// todo: currently ignore thg space limit
	spaceLimit uint64
	availSpace uint64
	//	Certain metrics such as similarity used in vector
	//	computations, along with metadata within the memory space.
	computeMetric *compute.QualityMetrics
}

// ---------------------------memory operation----------------------------

func (ms *MemSpace) AddMemory(key string, data []byte) error {
	return nil
}
func (ms *MemSpace) GetMemory(key string) ([]byte, error) {
	return nil, nil
}
func (ms *MemSpace) UpdateMemory(key string, data []byte) error {
	return nil
}
func (ms *MemSpace) DeleteMemory(key string) error {
	return nil
}
func (ms *MemSpace) ListMemories() []string {
	return nil
}

// ---------------------------agent operation----------------------------

func (ms *MemSpace) BindAgent(agentID uint64) error {
	return nil
}
func (ms *MemSpace) UnbindAgent(agentID uint64) error {
	return nil
}

func (ms *MemSpace) GetBoundAgents() []uint64 {
	return ms.bindingAgents
}
func (ms *MemSpace) IsAgentBound(agentID uint64) bool {
	return false
}

// canBinding space can binding?
func (ms *MemSpace) canBinding() bool {
	return false
}

// ---------------------------service operation----------------------------

func (ms *MemSpace) SearchByVector(queryVector []float32, topK int) error {
	return nil
}
func (ms *MemSpace) SemanticSearch(queryText string, topK int) error {
	return nil
}
