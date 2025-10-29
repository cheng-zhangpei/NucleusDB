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
	timestamp uint64
}
type TempMemUnit struct {
	// the content of the temp conversation
	value string
	//the timestamp of the content
	timestamp uint64
}

func NewMemUint(key []byte, value []byte, unitType ComputeType) *MemUint {
	now := time.Now()
	return &MemUint{
		key:       key,
		value:     value,
		unitType:  unitType,
		timestamp: uint64(now.UnixMilli()),
	}
}
func NewTempMemUint(value []byte) *MemUint {
	now := time.Now()
	return &MemUint{
		value:     value,
		timestamp: uint64(now.UnixMilli()),
	}
}

// GetMemoryValue get the memory of the MemUint
func (mu *MemUint) GetMemoryValue() string {
	return string(mu.value)
}

// convert translate tempUint to MemUint,path is the key of the memspace its belong to
func (tmu *TempMemUnit) convert(path string, computeType ComputeType) *MemUint {
	t := time.Now()
	milliTimestamp := uint64(t.UnixMilli())
	key := fmt.Sprintf("%s/%d", path, milliTimestamp)
	memUint := NewMemUint([]byte(key), []byte(tmu.value), computeType)
	return memUint
}
