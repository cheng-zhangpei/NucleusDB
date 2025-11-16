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

func DecodeMMSpace(data []byte) (*MemSpace, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	space := &MemSpace{
		BindingAgents: make([]uint64, 0),
		memUints:      make([]*MemUint, 0),
		TempMemUnits:  make([]*TempMemUnit, 0),  // 不编码，初始化为空
		vectorUints:   make([]*VectorRecord, 0), // 不编码，初始化为空
	}

	index := 0

	// 读取 MemSpaceId
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for MemSpaceId")
	}
	space.MemSpaceId = binary.LittleEndian.Uint64(data[index : index+8])
	index += 8

	// 读取 bindingAgents 的长度
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for bindingAgents length")
	}
	bindingAgentsSize := int(binary.LittleEndian.Uint64(data[index : index+8]))
	index += 8

	// 读取 bindingAgents
	if index+bindingAgentsSize*8 > len(data) {
		return nil, fmt.Errorf("insufficient data for bindingAgents")
	}
	space.BindingAgents = make([]uint64, bindingAgentsSize)
	for i := 0; i < bindingAgentsSize; i++ {
		space.BindingAgents[i] = binary.LittleEndian.Uint64(data[index : index+8])
		index += 8
	}

	// 读取 memUints 的长度
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for memUints length")
	}
	memUintsSize := int(binary.LittleEndian.Uint64(data[index : index+8]))
	index += 8

	// 读取每个 MemUint
	space.memUints = make([]*MemUint, memUintsSize)
	for i := 0; i < memUintsSize; i++ {
		memUnit := &MemUint{}

		// 读取 key
		if index+8 > len(data) {
			return nil, fmt.Errorf("insufficient data for key length")
		}
		keySize := int(binary.LittleEndian.Uint64(data[index : index+8]))
		index += 8

		if index+keySize > len(data) {
			return nil, fmt.Errorf("insufficient data for key")
		}
		memUnit.key = make([]byte, keySize)
		copy(memUnit.key, data[index:index+keySize])
		index += keySize

		// 读取 value
		if index+8 > len(data) {
			return nil, fmt.Errorf("insufficient data for value length")
		}
		valueSize := int(binary.LittleEndian.Uint64(data[index : index+8]))
		index += 8

		if index+valueSize > len(data) {
			return nil, fmt.Errorf("insufficient data for value")
		}
		memUnit.value = make([]byte, valueSize)
		copy(memUnit.value, data[index:index+valueSize])
		index += valueSize

		// 读取 unitType
		if index+8 > len(data) {
			return nil, fmt.Errorf("insufficient data for unitType")
		}
		memUnit.unitType = ComputeType(binary.LittleEndian.Uint64(data[index : index+8]))
		index += 8

		// 读取 timestamp
		if index+8 > len(data) {
			return nil, fmt.Errorf("insufficient data for timestamp")
		}
		memUnit.timestamp = binary.LittleEndian.Uint64(data[index : index+8])
		index += 8

		space.memUints[i] = memUnit
	}

	// 读取 spaceType
	spaceType, bytesRead := binary.Varint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("invalid spaceType")
	}
	space.spaceType = MemSpaceType(spaceType)
	index += bytesRead

	// 读取 spaceStatus
	spaceStatus, bytesRead := binary.Varint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("invalid spaceStatus")
	}
	space.spaceStatus = MemSpaceStatus(spaceStatus)
	index += bytesRead

	// 读取 spaceLimit
	spaceLimit, bytesRead := binary.Uvarint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("invalid spaceLimit")
	}
	space.spaceLimit = spaceLimit
	index += bytesRead

	// 读取 availSpace
	availSpace, bytesRead := binary.Uvarint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("invalid availSpace")
	}
	space.availSpace = availSpace
	index += bytesRead

	// 读取 description
	descSize, bytesRead := binary.Uvarint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("invalid description length")
	}
	index += bytesRead

	if index+int(descSize) > len(data) {
		return nil, fmt.Errorf("insufficient data for description")
	}
	space.description = string(data[index : index+int(descSize)])
	index += int(descSize)

	// 读取 name
	nameSize, bytesRead := binary.Uvarint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("invalid name length")
	}
	index += bytesRead

	if index+int(nameSize) > len(data) {
		return nil, fmt.Errorf("insufficient data for name")
	}
	space.name = string(data[index : index+int(nameSize)])
	index += int(nameSize)

	// 读取 memSpaceContentType
	contentType, bytesRead := binary.Varint(data[index:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("invalid memSpaceContentType")
	}
	space.memSpaceContentType = MemSpaceContentType(contentType)

	return space, nil
}
