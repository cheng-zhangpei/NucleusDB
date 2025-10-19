package memspace

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
	CreateAgentId uint64
	// MemSpaceId can not repeat in a system
	MemSpaceId uint64
	// allow multi-agent binding
	bindingAgents []uint64
	// persistent memory uint layout
	memUints []*MemUint
	// content of temp conversation
	TempMemUnits []*TempMemUnit
	// vector datatype record
	vectorUints []*VectorRecord
	// the type of the memSpace
	spaceType MemSpaceType
	// status
	spaceStatus MemSpaceStatus
	spaceLimit  uint64
	availSpace  uint64
	//	Certain metrics such as similarity used in vector
	//	computations, along with metadata within the memory space.
	//computeMetric *compute.QualityMetrics
}

func NewMemSpace() *MemSpace {
	return &MemSpace{
		CreateAgentId: 0,
		MemSpaceId:    0,
		bindingAgents: make([]uint64, 0),
		memUints:      make([]*MemUint, 0),
		TempMemUnits:  make([]*TempMemUnit, 0),
		vectorUints:   make([]*VectorRecord, 0),
		spaceType:     Private,
		spaceStatus:   Pending,
		spaceLimit:    0,
		availSpace:    0,
		//computeMetric: &compute.QualityMetrics{},
	}
}

// ---------------------------Persist memory operation: I want this part focus on memory record operations----------------------------

func (ms *MemSpace) PersistMemoryUint(key string, data []byte) error {
	return nil
}
func (ms *MemSpace) GetPersistMemoryUint(key string) ([]byte, error) {
	return nil, nil
}
func (ms *MemSpace) UpdatePersistMemory(key string, data []byte) error {
	return nil
}
func (ms *MemSpace) DeletePersistMemory(key string) error {
	return nil
}
func (ms *MemSpace) ListPersistMemories() []string {
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
