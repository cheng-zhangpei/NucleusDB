package memspace

// MemMetaData record some meta infomation in memspace
type MemMetaData struct {
	MemSpaceId    uint64
	CreateAgentId uint64
	// allow multi-agent binding
	BindingAgents []uint64
	SpaceType     MemSpaceType
	// status
	SpaceStatus MemSpaceStatus
	// uint: B
	SpaceLimit uint64
	AvailSpace uint64
}

func NewMemMetaData(id uint64, spaceType MemSpaceType, spaceLimit uint64) *MemMetaData {
	return &MemMetaData{
		CreateAgentId: 0,
		BindingAgents: make([]uint64, 0),
		SpaceType:     spaceType,

		SpaceStatus: Pending,
		SpaceLimit:  spaceLimit,
		AvailSpace:  spaceLimit,
		MemSpaceId:  id,
	}
}

// saveMetaData save the meta into database
func (memMetaData *MemMetaData) saveMetaData() error {
	return nil
}
