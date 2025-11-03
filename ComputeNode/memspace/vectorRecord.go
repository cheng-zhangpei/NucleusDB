package memspace

// VectorRecord vector data define
type VectorRecord struct {
	agentId uint64
	data    []float32
}

func NewVectorRecord(agentId uint64, data []float32) *VectorRecord {
	return &VectorRecord{
		agentId: agentId,
		data:    data,
	}
}
