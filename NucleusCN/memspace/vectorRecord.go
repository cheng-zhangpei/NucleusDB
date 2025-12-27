package memspace

type VectorRecord struct {
	agentId uint64
	data    []float32
	Content string // <--- Add this field to store the actual text
}

func NewVectorRecord(agentId uint64, data []float32, content string) *VectorRecord {
	return &VectorRecord{
		agentId: agentId,
		data:    data,
		Content: content,
	}
}
