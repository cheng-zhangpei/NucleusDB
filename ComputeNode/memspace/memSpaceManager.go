package memspace

import (
	"ComputeNode/client"
)

type MemSpaceManager struct {
	// storage client
	dbClient *client.NucleusClient
	// memspace meta addr
	metaKey string
	// private memspace
	privateTable map[string]*MemSpace
	// share memsapce
	publicTable map[string]*MemSpace
	// meta space:
	metaTable []*MemMetaData
}

func NewMemSpaceManager(dbClient *client.NucleusClient, metaKey string) (*MemSpaceManager, error) {
	// 这两个表是惰性加载的
	privateTable := make(map[string]*MemSpace)
	publicTable := make(map[string]*MemSpace)
	list, err := dbClient.DistributePrefixList([]byte(metaKey))
	if err != nil {
		return nil, err
	}
	metaList, err := DecodeMMMetaList(list)
	return &MemSpaceManager{
		privateTable: privateTable,
		publicTable:  publicTable,
		dbClient:     dbClient,
		metaKey:      metaKey,
		metaTable:    metaList,
	}, nil
}

// =========================================memory space operation===========================================
// registerMemSpace
func (msm *MemSpaceManager) registerMemSpace() error {
	// 创建元数据并持久化（与下面的内容要放到事务中中操作）
	// 创建记忆空间并持久化
	return nil
}

func (msm *MemSpaceManager) loadMemSpace() error {
	return nil
}

func (msm *MemSpaceManager) clearMemSpace() error {
	return nil
}
