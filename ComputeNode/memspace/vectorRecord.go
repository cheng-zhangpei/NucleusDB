package memspace

// 这个玩意需要如何定义呢？
type VectorRecord struct {
	agentId uint64
}

func NewVectorRecord() *VectorRecord {
	return &VectorRecord{}
}
