package memspace

import (
	"ComputeNode/client"
	"fmt"
)

type MemSpaceManager struct {
	// storage client
	dbClient *client.NucleusClient
	// private memspace
	privateTable map[string]*MemSpace
	// share memsapce
	publicTable map[string]*MemSpace
	// meta space:
	metaTable []*MemMetaData
	// path - 1, private table key
	privatePath string
	// path - 2, meta data path
	metaPath string
	// path - 3, shared path
	sharedPath string
}

func NewMemSpaceManager(dbClient *client.NucleusClient, privatePath string,
	metaPath string, sharedPath string) (*MemSpaceManager, error) {
	// 这两个表是惰性加载的
	privateTable := make(map[string]*MemSpace)
	publicTable := make(map[string]*MemSpace)
	list, err := dbClient.DistributePrefixList([]byte(metaPath))
	if err != nil {
		return nil, err
	}
	metaList, err := DecodeMMMetaList(list)
	return &MemSpaceManager{
		privateTable: privateTable,
		publicTable:  publicTable,
		dbClient:     dbClient,
		metaTable:    metaList,
		privatePath:  privatePath,
		metaPath:     metaPath,
		sharedPath:   sharedPath,
	}, nil
}

// =========================================memory space operation===========================================
// registerMemSpace
func (msm *MemSpaceManager) registerMemSpace(id uint64, spaceType MemSpaceType, spaceLimit uint64) error {
	// 创建元数据并持久化（与下面的内容要放到事务中中操作）
	metaData := NewMemMetaData(id, spaceType, spaceLimit)
	msm.metaTable = append(msm.metaTable, metaData)
	metaKey := fmt.Sprintf("%s/%d", msm.metaPath, id)
	mmSpace := NewMemSpace(id, spaceType, spaceLimit)
	var path string
	if spaceType == Private {
		path = fmt.Sprintf("%s/%d", msm.privatePath, id)
		msm.privateTable[path] = mmSpace
	}
	if spaceType == Shared {
		path = fmt.Sprintf("%s/%d", msm.sharedPath, id)
		msm.publicTable[path] = mmSpace
	}
	// 元数据持久化
	metaByte := EncodeMMMeta(metaData)
	err := msm.dbClient.TxnPut([]byte(metaKey), metaByte)
	if err != nil {
		return err
	}
	// 记忆空间持久化
	spaceByte, err := EncodeMMSpace(mmSpace)
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

// todo 在完善了MemSpace的计算细节之后再进行补充
func (msm *MemSpaceManager) loadMemSpace(mmId uint64) error {

	return nil
}

func (msm *MemSpaceManager) clearMemSpace() error {
	return nil
}
