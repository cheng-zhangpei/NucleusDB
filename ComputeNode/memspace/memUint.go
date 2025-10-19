package memspace

import "time"

type ComputeType int

const (
	Vector ComputeType = iota
	Graph
	// ... I hope our system can support multi-type
)

// temp memory
type MemUint struct {
	//Considering sharing issues, all memories records require agentId identification.
	agentId        uint64
	key            []byte
	value          []byte
	unitType       ComputeType
	lastUpdateTime time.Time
	// todo 后续应该会根据组织形式修改单个元素的布局
}
type TempMemUnit struct {
	agentId uint64
	// the content of the temp conversation
	value string
	//the timestamp of the content
	timestamp time.Time
}

func NewMemUint(key []byte, value []byte, unitType ComputeType) *MemUint {
	return &MemUint{}
}

// GetMemoryValue get the memory of the MemUint
func (mu *MemUint) GetMemoryValue() string {
	return string(mu.value)
}
