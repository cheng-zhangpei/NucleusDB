package memspace

import (
	"encoding/binary"
	"fmt"
)

// vector encoder

func EncodeMMMeta(mm *MemMetaData) []byte {
	bindingAgentsSize := len(mm.BindingAgents)

	// 预估 buffer 大小
	bufSize := 8 + // MemSpaceId
		8 + // bindingAgents 长度
		bindingAgentsSize*8 + // bindingAgents 数据
		binary.MaxVarintLen64*4 // spaceType, status, limit, avail

	buf := make([]byte, bufSize)
	index := 0

	// MemSpaceId
	binary.LittleEndian.PutUint64(buf[index:], mm.MemSpaceId)
	index += 8

	// bindingAgents 长度
	binary.LittleEndian.PutUint64(buf[index:], uint64(bindingAgentsSize))
	index += 8

	// bindingAgents 内容
	for _, id := range mm.BindingAgents {
		binary.LittleEndian.PutUint64(buf[index:], id)
		index += 8
	}

	// 变长整数
	index += binary.PutVarint(buf[index:], int64(mm.SpaceType))
	index += binary.PutVarint(buf[index:], int64(mm.SpaceStatus))
	index += binary.PutUvarint(buf[index:], mm.SpaceLimit)
	index += binary.PutUvarint(buf[index:], mm.AvailSpace)

	return buf[:index]
}
func EncodeMMSpace(space *MemSpace) ([]byte, error) {
	if space == nil {
		return nil, fmt.Errorf("space is nil")
	}

	bufSize := estimateMMSpaceBufferSize(space)
	buf := make([]byte, bufSize)
	index := 0

	// MemSpaceId
	binary.LittleEndian.PutUint64(buf[index:index+8], space.MemSpaceId)
	index += 8

	// bindingAgents 长度 + 数据
	bindingSize := len(space.BindingAgents)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(bindingSize))
	index += 8
	for _, id := range space.BindingAgents {
		if index+8 > len(buf) {
			return nil, fmt.Errorf("buffer overflow in bindingAgents")
		}
		binary.LittleEndian.PutUint64(buf[index:index+8], id)
		index += 8
	}

	// memUints 长度
	memUintsSize := len(space.memUints)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(memUintsSize))
	index += 8

	// 编码每个 MemUint
	for _, mu := range space.memUints {
		if mu == nil {
			mu = &MemUint{}
		}
		key := mu.key
		value := mu.value
		keySize := len(key)
		valueSize := len(value)

		// key size + data
		binary.LittleEndian.PutUint64(buf[index:index+8], uint64(keySize))
		index += 8
		if index+keySize > len(buf) {
			return nil, fmt.Errorf("buffer overflow in key data")
		}
		copy(buf[index:index+keySize], key)
		index += keySize

		// value size + data
		binary.LittleEndian.PutUint64(buf[index:index+8], uint64(valueSize))
		index += 8
		if index+valueSize > len(buf) {
			return nil, fmt.Errorf("buffer overflow in value data")
		}
		copy(buf[index:index+valueSize], value)
		index += valueSize

		// unitType (uint64)
		binary.LittleEndian.PutUint64(buf[index:index+8], uint64(mu.unitType))
		index += 8

		// timestamp (uint64)
		binary.LittleEndian.PutUint64(buf[index:index+8], mu.timestamp)
		index += 8
	}

	// spaceType (Varint)
	n := binary.PutVarint(buf[index:], int64(space.spaceType))
	if n <= 0 {
		return nil, fmt.Errorf("failed to encode spaceType")
	}
	index += n

	// spaceStatus (Varint)
	n = binary.PutVarint(buf[index:], int64(space.spaceStatus))
	if n <= 0 {
		return nil, fmt.Errorf("failed to encode spaceStatus")
	}
	index += n

	// spaceLimit (Uvarint)
	n = binary.PutUvarint(buf[index:], space.spaceLimit)
	if n == 0 {
		return nil, fmt.Errorf("failed to encode spaceLimit")
	}
	index += n

	// availSpace (Uvarint)
	n = binary.PutUvarint(buf[index:], space.availSpace)
	if n == 0 {
		return nil, fmt.Errorf("failed to encode availSpace")
	}
	index += n

	// description (string with uvarint length)
	descBytes := []byte(space.description)
	descLen := uint64(len(descBytes))
	n = binary.PutUvarint(buf[index:], descLen)
	if n == 0 {
		return nil, fmt.Errorf("failed to encode description length")
	}
	index += n
	if index+int(descLen) > len(buf) {
		return nil, fmt.Errorf("buffer overflow in description")
	}
	copy(buf[index:index+int(descLen)], descBytes)
	index += int(descLen)

	// name
	nameBytes := []byte(space.name)
	nameLen := uint64(len(nameBytes))
	n = binary.PutUvarint(buf[index:], nameLen)
	if n == 0 {
		return nil, fmt.Errorf("failed to encode name length")
	}
	index += n
	if index+int(nameLen) > len(buf) {
		return nil, fmt.Errorf("buffer overflow in name")
	}
	copy(buf[index:index+int(nameLen)], nameBytes)
	index += int(nameLen)

	// memSpaceContentType (Varint)
	n = binary.PutVarint(buf[index:], int64(space.memSpaceContentType))
	if n <= 0 {
		return nil, fmt.Errorf("failed to encode memSpaceContentType")
	}
	index += n

	// flushTime (int) -> 编码为 int64
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(space.flushTime))
	index += 8

	// tempIndexPtr (uint64)
	binary.LittleEndian.PutUint64(buf[index:index+8], space.tempIndexPtr)
	index += 8

	// tempSpaceSize (uint64)
	binary.LittleEndian.PutUint64(buf[index:index+8], space.tempSpaceSize)
	index += 8

	// persistKey (string with uvarint length)
	persistKeyBytes := []byte(space.persistKey)
	pkLen := uint64(len(persistKeyBytes))
	n = binary.PutUvarint(buf[index:], pkLen)
	if n == 0 {
		return nil, fmt.Errorf("failed to encode persistKey length")
	}
	index += n
	if index+int(pkLen) > len(buf) {
		return nil, fmt.Errorf("buffer overflow in persistKey")
	}
	copy(buf[index:index+int(pkLen)], persistKeyBytes)
	index += int(pkLen)

	return buf[:index], nil
}
func estimateMMSpaceBufferSize(space *MemSpace) int {
	size := 8 + 8 + len(space.BindingAgents)*8 // MemSpaceId + binding size + agents

	size += 8 // memUints 长度
	for _, mu := range space.memUints {
		keySize := len(mu.key)
		valueSize := len(mu.value)
		size += 8 + keySize + 8 + valueSize + 8 + 8 // key/value len+data + type + timestamp
	}

	size += binary.MaxVarintLen64 * 5 // spaceType, status, limit, avail, contentType
	size += binary.MaxVarintLen64 * 3 // descLen, nameLen, persistKeyLen
	size += len(space.description) + len(space.name) + len(space.persistKey)

	size += 8 + 8 + 8 + 8 // flushTime, tempIndexPtr, tempSpaceSize, plus padding

	return size + 256 // 额外缓冲
}
