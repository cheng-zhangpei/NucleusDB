package memspace

// MemMetaData record some meta infomation in memspace
type MemMetaData struct {
	MemSpaceId    uint64
	CreateAgentId uint64
	// allow multi-agent binding
	BindingAgents []uint64
	SpaceType     *MemSpaceType
	// status
	SpaceStatus *MemSpaceStatus
	SpaceLimit  uint64
	AvailSpace  uint64
}

func NewMemMetaData() *MemMetaData {
	return &MemMetaData{
		CreateAgentId: 0,
		BindingAgents: make([]uint64, 0),
		SpaceType:     nil,
		SpaceStatus:   nil,
		SpaceLimit:    0,
		AvailSpace:    0,
	}
}

// saveMetaData save the meta into database
func (memMetaData *MemMetaData) saveMetaData() error {
	return nil
}
