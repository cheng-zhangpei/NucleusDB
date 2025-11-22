package memspace

import (
	"encoding/binary"
	"fmt"
	"sync"
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
	mm := NewMemMetaData(0, Private, 0)
	index := 0

	// 读取 MemSpaceId
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for MemSpaceId")
	}
	mm.MemSpaceId = binary.LittleEndian.Uint64(data[index : index+8])
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
		BindingAgents:         make([]uint64, 0),
		memUints:              make([]*MemUint, 0),
		TempMemUnits:          make([]*TempMemUnit, 0),  // 不编码，留空
		vectorUints:           make([]*VectorRecord, 0), // 不编码
		embeddingServerClient: nil,                      // 运行时对象，不编码
		stopFlush:             nil,                      // chan 不编码
		dbClient:              nil,                      // db client 不编码
		mu:                    &sync.RWMutex{},          // 新建锁
	}

	index := 0

	// MemSpaceId
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for MemSpaceId")
	}
	space.MemSpaceId = binary.LittleEndian.Uint64(data[index : index+8])
	index += 8

	// bindingAgents 长度
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for bindingAgents length")
	}
	bindingSize := int(binary.LittleEndian.Uint64(data[index : index+8]))
	index += 8

	// bindingAgents 数据
	space.BindingAgents = make([]uint64, bindingSize)
	for i := 0; i < bindingSize; i++ {
		if index+8 > len(data) {
			return nil, fmt.Errorf("insufficient data for bindingAgents[%d]", i)
		}
		space.BindingAgents[i] = binary.LittleEndian.Uint64(data[index : index+8])
		index += 8
	}

	// memUints 长度
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for memUints length")
	}
	memUintsSize := int(binary.LittleEndian.Uint64(data[index : index+8]))
	index += 8

	// memUints 数据
	space.memUints = make([]*MemUint, 0, memUintsSize)
	for i := 0; i < memUintsSize; i++ {
		mu := &MemUint{}

		// key 长度
		if index+8 > len(data) {
			return nil, fmt.Errorf("insufficient data for key length")
		}
		keySize := int(binary.LittleEndian.Uint64(data[index : index+8]))
		index += 8

		if index+keySize > len(data) {
			return nil, fmt.Errorf("insufficient data for key")
		}
		mu.key = make([]byte, keySize)
		copy(mu.key, data[index:index+keySize])
		index += keySize

		// value 长度
		if index+8 > len(data) {
			return nil, fmt.Errorf("insufficient data for value length")
		}
		valueSize := int(binary.LittleEndian.Uint64(data[index : index+8]))
		index += 8

		if index+valueSize > len(data) {
			return nil, fmt.Errorf("insufficient data for value")
		}
		mu.value = make([]byte, valueSize)
		copy(mu.value, data[index:index+valueSize])
		index += valueSize

		// unitType
		if index+8 > len(data) {
			return nil, fmt.Errorf("insufficient data for unitType")
		}
		mu.unitType = ComputeType(binary.LittleEndian.Uint64(data[index : index+8]))
		index += 8

		// timestamp
		if index+8 > len(data) {
			return nil, fmt.Errorf("insufficient data for timestamp")
		}
		mu.timestamp = binary.LittleEndian.Uint64(data[index : index+8])
		index += 8

		space.memUints = append(space.memUints, mu)
	}

	// spaceType
	v, n := binary.Varint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid spaceType")
	}
	space.spaceType = MemSpaceType(v)
	index += n

	// spaceStatus
	v, n = binary.Varint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid spaceStatus")
	}
	space.spaceStatus = MemSpaceStatus(v)
	index += n

	// spaceLimit
	limit, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid spaceLimit")
	}
	space.spaceLimit = limit
	index += n

	// availSpace
	avail, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid availSpace")
	}
	space.availSpace = avail
	index += n

	// description
	descLen, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid description length")
	}
	index += n
	if index+int(descLen) > len(data) {
		return nil, fmt.Errorf("insufficient data for description")
	}
	space.description = string(data[index : index+int(descLen)])
	index += int(descLen)

	// name
	nameLen, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid name length")
	}
	index += n
	if index+int(nameLen) > len(data) {
		return nil, fmt.Errorf("insufficient data for name")
	}
	space.name = string(data[index : index+int(nameLen)])
	index += int(nameLen)

	// memSpaceContentType
	ct, n := binary.Varint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid memSpaceContentType")
	}
	space.memSpaceContentType = MemSpaceContentType(ct)
	index += n

	// flushTime (int)
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for flushTime")
	}
	space.flushTime = int(binary.LittleEndian.Uint64(data[index : index+8]))
	index += 8

	// tempIndexPtr
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for tempIndexPtr")
	}
	space.tempIndexPtr = binary.LittleEndian.Uint64(data[index : index+8])
	index += 8

	// tempSpaceSize
	if index+8 > len(data) {
		return nil, fmt.Errorf("insufficient data for tempSpaceSize")
	}
	space.tempSpaceSize = binary.LittleEndian.Uint64(data[index : index+8])
	index += 8

	// persistKey
	pkLen, n := binary.Uvarint(data[index:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid persistKey length")
	}
	index += n
	if index+int(pkLen) > len(data) {
		return nil, fmt.Errorf("insufficient data for persistKey")
	}
	space.persistKey = string(data[index : index+int(pkLen)])

	return space, nil
}
