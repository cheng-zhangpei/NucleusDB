package memspace

import (
	"encoding/json"
	"fmt"
	"time"
)

type MemSpaceManager struct {
	// storage dbClient
	dbClient *NucleusClient
	// private memspace
	PrivateTable map[uint64]*MemSpace
	// share memsapce
	PublicTable map[uint64]*MemSpace
	// meta space:
	metaTable map[uint64]*MemMetaData
	// path - 1, private table key
	privatePath string
	// path - 2, meta data path
	metaPath string
	// path - 3, shared path
	sharedPath string
	eventChan  chan MemEvent
}

// MemEvent :Persist attribute requiring persistence
type MemEvent struct {
	MemSpaceId uint64
	// the type of the memSpace
	spaceType MemSpaceType
	// status
	spaceStatus MemSpaceStatus
	spaceLimit  uint64
	availSpace  uint64
	// Memory Space description
	description string

	name                string
	memSpaceContentType MemSpaceContentType
	flushTime           int
	tempIndexPtr        uint64 // the latest/update position of the temp memory space
	tempSpaceSize       uint64 // indicate the size of the temp memSpace
	persistKey          string // record the area the memspace persist the memUint
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
		PrivateTable: privateTable,
		PublicTable:  publicTable,
		dbClient:     dbClient,
		metaTable:    metaTable,
		privatePath:  privatePath,
		metaPath:     metaPath,
		sharedPath:   sharedPath,
	}, nil
}

// =========================================memory space operation===========================================

// RegisterMemSpace register a memSpace
func (msm *MemSpaceManager) RegisterMemSpace(id uint64, spaceType MemSpaceType, spaceLimit uint64,
	memSpaceContentType MemSpaceContentType, embeddingClientAddr string, flushTime int, tempMemSpaceSize uint64) error {
	// 保存元数据
	metaData := NewMemMetaData(id, spaceType, spaceLimit)
	msm.metaTable[id] = metaData
	metaKey := fmt.Sprintf("%s/%d", msm.metaPath, id)
	// 由类型决定
	var memPath string = ""
	if spaceType == Shared {
		memPath = msm.sharedPath
	} else {
		memPath = msm.privatePath
	}
	mmSpace := NewMemSpace(id, spaceType, spaceLimit, memSpaceContentType, embeddingClientAddr,
		flushTime, tempMemSpaceSize, memPath, msm.dbClient, msm.eventChan)
	var path string
	if spaceType == Private {
		msm.PrivateTable[id] = mmSpace
	}
	if spaceType == Shared {
		msm.PublicTable[id] = mmSpace
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
		msm.PrivateTable[mmId] = msp
	} else if msp.spaceType == Shared {
		msm.PublicTable[mmId] = msp
	}
	return nil
}

// clearMemSpace clear a specific memspace
func (msm *MemSpaceManager) clearMemSpace(mmId uint64) error {
	_, exist := msm.PrivateTable[mmId]
	if !exist {
		return ErrMemSpaceNotExist
	} else {
		msm.PrivateTable[mmId] = nil
	}
	pms, exist := msm.PublicTable[mmId]
	if !exist {
		return ErrMemSpaceNotExist
	} else {
		// when the no agent binding in the memorySpace
		if len(pms.BindingAgents) == 0 {
			msm.PublicTable[mmId] = nil
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

// CanBindingPrivate  if the memId existed in the privateMem or sharedMem
func (msm *MemSpaceManager) CanBindingPrivate(id uint64) bool {
	pms, exist := msm.PrivateTable[id]
	if !exist {
		return false
	} else {
		// if the private memspace is pending?
		if len(pms.BindingAgents) != 0 {
			return false
		}
		return true
	}
}

// CanBindingPublic if agent can bind a shared memspace
func (msm *MemSpaceManager) CanBindingPublic(id uint64) bool {
	_, exist := msm.PublicTable[id]
	if !exist {
		return false
	}
	return true
}

func (msm *MemSpaceManager) PersistMemMeta() error {
	return nil
}

// BuildMemSpaceByMeta 使用内存中的meta数据重建记忆空间
func (msm *MemSpaceManager) BuildMemSpaceByMeta() error {

	return nil
}

func (msm *MemSpaceManager) StartEventProcessor() {
	go func() {
		for event := range msm.eventChan {
			msm.handleEvent(event)
		}
	}()
}

// handleEvent handle meta data update request
func (msm *MemSpaceManager) handleEvent(event MemEvent) {
	id := event.MemSpaceId
	meta := msm.metaTable[id]

	if meta == nil {
		meta = NewMemMetaData(id, event.spaceType, event.spaceLimit)
		msm.metaTable[id] = meta
	}

	if event.spaceType != 0 {
		meta.SpaceType = event.spaceType
	}

	meta.SpaceStatus = event.spaceStatus
	meta.SpaceLimit = event.spaceLimit
	meta.AvailSpace = event.availSpace
	meta.Description = event.description
	meta.Name = event.name
	meta.MemSpaceContentType = event.memSpaceContentType
	meta.FlushTime = event.flushTime
	meta.TempIndexPtr = event.tempIndexPtr
	meta.TempSpaceSize = event.tempSpaceSize
	meta.PersistKey = event.persistKey
	meta.UpdatedAt = time.Now().Unix()

	msm.persistMetaToDB(meta)
}
func (msm *MemSpaceManager) persistMetaToDB(meta *MemMetaData) {
	data, err := json.Marshal(meta)
	if err != nil {
		// log error
		return
	}
	key := fmt.Sprintf("%s/%d", msm.metaPath, meta.MemSpaceId)
	_ = msm.dbClient.DistributePut([]byte(key), data)
}
