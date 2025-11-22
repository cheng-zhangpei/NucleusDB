package memspace

import "time"

// MemMetaData record some meta infomation in memspace
type MemMetaData struct {
	MemSpaceId          uint64
	BindingAgents       []uint64
	SpaceType           MemSpaceType
	SpaceStatus         MemSpaceStatus
	SpaceLimit          uint64
	AvailSpace          uint64
	Description         string
	Name                string
	MemSpaceContentType MemSpaceContentType
	FlushTime           int
	TempIndexPtr        uint64
	TempSpaceSize       uint64
	PersistKey          string
	UpdatedAt           int64 // 记录最后更新时间
}

func NewMemMetaData(id uint64, spaceType MemSpaceType, spaceLimit uint64) *MemMetaData {
	return &MemMetaData{
		MemSpaceId:          id,
		BindingAgents:       make([]uint64, 0),
		SpaceType:           spaceType,
		SpaceStatus:         Pending, // 假设 Pending 是初始状态常量
		SpaceLimit:          spaceLimit,
		AvailSpace:          spaceLimit,
		Description:         "",
		Name:                "",
		MemSpaceContentType: 0,
		FlushTime:           0,
		TempIndexPtr:        0,
		TempSpaceSize:       0,
		PersistKey:          "",
		UpdatedAt:           time.Now().Unix(),
	}
}

// saveMetaData save the meta into database
func (memMetaData *MemMetaData) saveMetaData() error {
	return nil
}
