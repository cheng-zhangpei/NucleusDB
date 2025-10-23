package memspace

import (
	"fmt"
	"time"
)

type ComputeType int

const (
	Vector ComputeType = iota
	Graph
	String
	// ... I hope our system can support multi-type
)

type MemUint struct {
	//Considering sharing issues, all memories records require agentId identification.
	// 这个key由几个部分组成：
	key       []byte
	value     []byte
	unitType  ComputeType
	timestamp time.Time
	// todo 后续应该会根据组织形式修改单个元素的布局
}
type TempMemUnit struct {
	// the content of the temp conversation
	value string
	//the timestamp of the content
	timestamp time.Time
}

func NewMemUint(key []byte, value []byte, unitType ComputeType) *MemUint {
	return &MemUint{
		key:       key,
		value:     value,
		unitType:  unitType,
		timestamp: time.Now(),
	}
}

// GetMemoryValue get the memory of the MemUint
func (mu *MemUint) GetMemoryValue() string {
	return string(mu.value)
}

// convert translate tempUint to MemUint,path is the key of the memspace its belong to
func (tmu *TempMemUnit) convert(path string, computeType ComputeType) *MemUint {
	t := time.Now()
	milliTimestamp := t.UnixMilli()
	key := fmt.Sprintf("%s/%d", path, milliTimestamp)
	memUint := NewMemUint([]byte(key), []byte(tmu.value), computeType)
	return memUint
}
