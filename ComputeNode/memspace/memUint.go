package memspace

import "time"

type ComputeType int

const (
	Vector ComputeType = iota
	Graph
	// ... I hope our system can support multi-type
)

// single mem recode
type MemUint struct {
	key            []byte
	value          []byte
	unitType       ComputeType
	lastUpdateTime time.Time
	// todo 后续应该会根据组织形式修改单个元素的布局

}

func NewMemUint(key []byte, value []byte, unitType ComputeType) *MemUint {
	return &MemUint{}
}
