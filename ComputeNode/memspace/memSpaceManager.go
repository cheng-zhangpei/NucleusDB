package memspace

import (
	"fmt"
)

type MemSpaceManager struct {
	// storage dbClient
	dbClient *NucleusClient
	// private memspace
	privateTable map[uint64]*MemSpace
	// share memsapce
	publicTable map[uint64]*MemSpace
	// meta space:
	metaTable map[uint64]*MemMetaData
	// path - 1, private table key
	privatePath string
	// path - 2, meta data path
	metaPath string
	// path - 3, shared path
	sharedPath string
}

func NewMemSpaceManager(dbClient *NucleusClient, privatePath string,
	metaPath string, sharedPath string) (*MemSpaceManager, error) {
	// 这两个表是惰性加载的
	privateTable := make(map[uint64]*MemSpace)
	publicTable := make(map[uint64]*MemSpace)
	list, err := dbClient.DistributePrefixList([]byte(metaPath))
	metaTable := make(map[uint64]*MemMetaData)
	if err != nil {
		return nil, err
	}
	metaList, err := DecodeMMMetaList(list)
	for _, meta := range metaList {
		metaTable[meta.MemSpaceId] = meta
	}

	return &MemSpaceManager{
		privateTable: privateTable,
		publicTable:  publicTable,
		dbClient:     dbClient,
		metaTable:    metaTable,
		privatePath:  privatePath,
		metaPath:     metaPath,
		sharedPath:   sharedPath,
	}, nil
}

// =========================================memory space operation===========================================
// registerMemSpace
func (msm *MemSpaceManager) registerMemSpace(id uint64, spaceType MemSpaceType, spaceLimit uint64,
	memSpaceContentType MemSpaceContentType, embeddingClientAddr string) error {
	// 保存元数据
	metaData := NewMemMetaData(id, spaceType, spaceLimit)
	msm.metaTable[id] = metaData
	metaKey := fmt.Sprintf("%s/%d", msm.metaPath, id)
	mmSpace := NewMemSpace(id, spaceType, spaceLimit, memSpaceContentType, embeddingClientAddr)
	var path string
	if spaceType == Private {
		msm.privateTable[id] = mmSpace
	}
	if spaceType == Shared {
		msm.publicTable[id] = mmSpace
	}
	// 元数据持久化
	metaByte := EncodeMMMeta(metaData)
	// 记忆空间持久化
	spaceByte, err := EncodeMMSpace(mmSpace)
	if err != nil {
		return err
	}

	err = msm.dbClient.TxnPut([]byte(metaKey), metaByte)
	if err != nil {
		return err
	}
	err = msm.dbClient.TxnPut([]byte(path), spaceByte)
	if err != nil {
		return err
	}
	err = msm.dbClient.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (msm *MemSpaceManager) loadMemSpace(mmId uint64) error {
	// 先找这个id是否在元数据中
	meta, exist := msm.metaTable[mmId]
	if !exist {
		return ErrMetaNotExist
	}
	// 通过meta的值来找将
	id := meta.MemSpaceId
	spaceType := meta.SpaceType
	var path string
	switch spaceType {
	case Private:
		path = fmt.Sprintf("%s/%d", msm.privatePath, id)
	case Shared:
		path = fmt.Sprintf("%s/%d", msm.sharedPath, id)
	default:
		return ErrMetaSpaceType
	}
	// 去查找数据
	memSpaceByte, err := msm.dbClient.DistributeGet([]byte(path))
	if err != nil {
		return err
	}
	msp, err := DecodeMMSpace(memSpaceByte)
	if err != nil {
		return err
	}
	// 将数据放入索引表中
	if msp.spaceType == Private {
		msm.privateTable[mmId] = msp
	} else if msp.spaceType == Shared {
		msm.publicTable[mmId] = msp
	}
	return nil
}

// clearMemSpace clear a specific memspace
func (msm *MemSpaceManager) clearMemSpace(mmId uint64) error {
	_, exist := msm.privateTable[mmId]
	if !exist {
		return ErrMemSpaceNotExist
	} else {
		msm.privateTable[mmId] = nil
	}
	pms, exist := msm.publicTable[mmId]
	if !exist {
		return ErrMemSpaceNotExist
	} else {
		// when the no agent binding in the memorySpace
		if len(pms.bindingAgents) == 0 {
			msm.publicTable[mmId] = nil
		}
	}
	return nil
}

// FindBehavioralMemory called by agent, to find binding BehavioralMemory
func (msm *MemSpaceManager) FindBehavioralMemory(agentId uint64) ([]*MemSpace, error) {

	return nil, nil
}

// FindContentMemory called by agent, to find binding ContentMemory
func (msm *MemSpaceManager) FindContentMemory(agentId uint64) ([]*MemSpace, error) {

	return nil, nil
}

// FindToolMemory called by agent, to find binding ToolMemory
func (msm *MemSpaceManager) FindToolMemory(agentId uint64) ([]*MemSpace, error) {

	return nil, nil
}
