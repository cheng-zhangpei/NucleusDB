package memspace

import (
	"encoding/binary"
	"fmt"
)

// vector decoder

func decodeVector(record VectorRecord) *MemUint {
	return NewMemUint([]byte("test"), []byte(""), Vector)
}
func DecodeMMMetaList(data [][]byte) ([]*MemMetaData, error) {
	metaData := make([]*MemMetaData, 0)
	for _, meta := range data {
		mmMeta, err := DecodeMMMeta(meta)
		if err != nil {
			return nil, err
		}
		metaData = append(metaData, mmMeta)
	}
	return metaData, nil
}
func DecodeMMMeta(data []byte) (*MemMetaData, error) {
	mm := &MemMetaData{
		0,
		0,
		nil,
		Private,
		Pending,
		0,
		0,
	}
	index := 0

	// 读取 MemSpaceId
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for MemSpaceId")
	}
	mm.MemSpaceId = binary.LittleEndian.Uint64(data[index : index+8])
	index += 8

	// 读取 CreateAgentId
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for CreateAgentId")
	}
	mm.CreateAgentId = binary.LittleEndian.Uint64(data[index : index+8])
	index += 8

	// 读取 bindingAgents 的长度
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for bindingAgents length")
	}
	bindingAgentsSize := int(binary.LittleEndian.Uint64(data[index : index+8]))
	index += 8

	// 读取 bindingAgents
	mm.BindingAgents = make([]uint64, bindingAgentsSize)
	for i := 0; i < bindingAgentsSize; i++ {
		if index+8 > len(data) {
			return nil, fmt.Errorf("insufficient data for bindingAgents[%d]", i)
		}
		mm.BindingAgents[i] = binary.LittleEndian.Uint64(data[index : index+8])
		index += 8
	}

	// 读取 spaceType
	spaceTypeVal, bytesRead := binary.Varint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("failed to read spaceType")
	}
	mm.SpaceType = MemSpaceType(spaceTypeVal)

	index += bytesRead

	// 读取 spaceStatus
	spaceStatusVal, bytesRead := binary.Varint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("failed to read spaceStatus")
	}

	mm.SpaceStatus = MemSpaceStatus(spaceStatusVal)
	index += bytesRead

	// 读取 spaceLimit
	spaceLimit, bytesRead := binary.Uvarint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("failed to read spaceLimit")
	}
	mm.SpaceLimit = spaceLimit
	index += bytesRead

	// 读取 availSpace
	availSpace, bytesRead := binary.Uvarint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("failed to read availSpace")
	}
	mm.AvailSpace = availSpace
	index += bytesRead

	return mm, nil
}

//func DecodeMMSpace([]byte metaMmspace) (*MemSpace,error) {
//
//	return nil,nil
//}
