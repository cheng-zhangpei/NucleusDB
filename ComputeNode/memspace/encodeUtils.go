package memspace

import (
	"encoding/binary"
	"fmt"
)

// vector encoder

func EncodeMMMeta(mm *MemMetaData) []byte {
	// 计算 bindingAgents 的长度
	bindingAgentsSize := len(mm.BindingAgents)

	// 计算缓冲区大小
	// MemSpaceId (8字节) + CreateAgentId (8字节) + bindingAgents长度 (8字节) + bindingAgents数据 (变长) +
	// spaceType (变长) + spaceStatus (变长) + spaceLimit (变长) + availSpace (变长)
	bufSize := 8 + 8 + 8 + bindingAgentsSize*8 +
		binary.MaxVarintLen64*4 // 4个变长字段

	buf := make([]byte, bufSize)
	index := 0

	// 存储 MemSpaceId (8字节，小端存储)
	binary.LittleEndian.PutUint64(buf[index:index+8], mm.MemSpaceId)
	index += 8

	// 存储 bindingAgents 的长度 (8字节，小端存储)
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(bindingAgentsSize))
	index += 8

	// 存储 bindingAgents 中的每个 agent ID (8字节，小端存储)
	for _, agentId := range mm.BindingAgents {
		binary.LittleEndian.PutUint64(buf[index:index+8], agentId)
		index += 8
	}

	// 存储 spaceType（变长编码）
	index += binary.PutVarint(buf[index:], int64(mm.SpaceType))

	// 存储 spaceStatus（变长编码）
	index += binary.PutVarint(buf[index:], int64(mm.SpaceStatus))

	// 存储 spaceLimit（变长编码）
	index += binary.PutUvarint(buf[index:], mm.SpaceLimit)

	// 存储 availSpace（变长编码）
	index += binary.PutUvarint(buf[index:], mm.AvailSpace)

	// 返回实际写入的字节数据
	return buf[:index]
}
func EncodeMMSpace(space *MemSpace) ([]byte, error) {
	// 计算 bindingAgents 的长度
	bindingAgentsSize := len(space.BindingAgents)

	// 计算 memUints 的长度
	memUintsSize := len(space.memUints)

	// 计算 description 和 name 的字节长度
	descBytes := []byte(space.description)
	nameBytes := []byte(space.name)
	descSize := len(descBytes)
	nameSize := len(nameBytes)

	// 重新计算缓冲区大小 - 更精确的计算
	bufSize := 0

	// 固定长度字段
	bufSize += 8 + 8 + 8 + (bindingAgentsSize * 8) + 8 // CreateAgentId + MemSpaceId + bindingAgents长度 + bindingAgents数据 + memUints长度

	// 计算每个 MemUint 的大小
	for _, memUnit := range space.memUints {
		keySize := len(memUnit.key)
		valueSize := len(memUnit.value)
		bufSize += 8 + keySize + 8 + valueSize + 8 + 8 // key长度 + key数据 + value长度 + value数据 + unitType + timestamp
	}

	// 变长字段的最大可能长度
	bufSize += binary.MaxVarintLen64 * 7 // 7个变长字段

	// 字符串数据
	bufSize += descSize + nameSize

	// 添加一些额外空间作为缓冲
	bufSize += 1024

	buf := make([]byte, bufSize)
	index := 0

	// 存储 MemSpaceId (8字节，小端存储)
	if index+8 > len(buf) {
		return nil, fmt.Errorf("buffer overflow at MemSpaceId")
	}
	binary.LittleEndian.PutUint64(buf[index:index+8], space.MemSpaceId)
	index += 8

	// 存储 bindingAgents 的长度 (8字节，小端存储)
	if index+8 > len(buf) {
		return nil, fmt.Errorf("buffer overflow at bindingAgents length")
	}
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(bindingAgentsSize))
	index += 8

	// 存储 bindingAgents 中的每个 agent ID (8字节，小端存储)
	for _, agentId := range space.BindingAgents {
		if index+8 > len(buf) {
			return nil, fmt.Errorf("buffer overflow at bindingAgents data")
		}
		binary.LittleEndian.PutUint64(buf[index:index+8], agentId)
		index += 8
	}

	// 存储 memUints 的长度 (8字节，小端存储)
	if index+8 > len(buf) {
		return nil, fmt.Errorf("buffer overflow at memUints length")
	}
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(memUintsSize))
	index += 8

	// 存储每个 MemUint
	for _, memUnit := range space.memUints {
		keySize := len(memUnit.key)
		valueSize := len(memUnit.value)

		// 存储 key 的长度和数据
		if index+8 > len(buf) {
			return nil, fmt.Errorf("buffer overflow at key length")
		}
		binary.LittleEndian.PutUint64(buf[index:index+8], uint64(keySize))
		index += 8

		if index+keySize > len(buf) {
			return nil, fmt.Errorf("buffer overflow at key data")
		}
		copy(buf[index:index+keySize], memUnit.key)
		index += keySize

		// 存储 value 的长度和数据
		if index+8 > len(buf) {
			return nil, fmt.Errorf("buffer overflow at value length")
		}
		binary.LittleEndian.PutUint64(buf[index:index+8], uint64(valueSize))
		index += 8

		if index+valueSize > len(buf) {
			return nil, fmt.Errorf("buffer overflow at value data")
		}
		copy(buf[index:index+valueSize], memUnit.value)
		index += valueSize

		// 存储 unitType
		if index+8 > len(buf) {
			return nil, fmt.Errorf("buffer overflow at unitType")
		}
		binary.LittleEndian.PutUint64(buf[index:index+8], uint64(memUnit.unitType))
		index += 8

		// 存储 timestamp
		if index+8 > len(buf) {
			return nil, fmt.Errorf("buffer overflow at timestamp")
		}
		binary.LittleEndian.PutUint64(buf[index:index+8], memUnit.timestamp)
		index += 8
	}

	// 存储 spaceType（变长编码）
	if index+binary.MaxVarintLen64 > len(buf) {
		return nil, fmt.Errorf("buffer overflow at spaceType")
	}
	n := binary.PutVarint(buf[index:], int64(space.spaceType))
	index += n

	// 存储 spaceStatus（变长编码）
	if index+binary.MaxVarintLen64 > len(buf) {
		return nil, fmt.Errorf("buffer overflow at spaceStatus")
	}
	n = binary.PutVarint(buf[index:], int64(space.spaceStatus))
	index += n

	// 存储 spaceLimit（变长编码）
	if index+binary.MaxVarintLen64 > len(buf) {
		return nil, fmt.Errorf("buffer overflow at spaceLimit")
	}
	n = binary.PutUvarint(buf[index:], space.spaceLimit)
	index += n

	// 存储 availSpace（变长编码）
	if index+binary.MaxVarintLen64 > len(buf) {
		return nil, fmt.Errorf("buffer overflow at availSpace")
	}
	n = binary.PutUvarint(buf[index:], space.availSpace)
	index += n

	// 存储 description 的长度和数据
	if index+binary.MaxVarintLen64 > len(buf) {
		return nil, fmt.Errorf("buffer overflow at description length")
	}
	n = binary.PutUvarint(buf[index:], uint64(descSize))
	index += n

	if index+descSize > len(buf) {
		return nil, fmt.Errorf("buffer overflow at description data")
	}
	copy(buf[index:index+descSize], descBytes)
	index += descSize

	// 存储 name 的长度和数据
	if index+binary.MaxVarintLen64 > len(buf) {
		return nil, fmt.Errorf("buffer overflow at name length")
	}
	n = binary.PutUvarint(buf[index:], uint64(nameSize))
	index += n

	if index+nameSize > len(buf) {
		return nil, fmt.Errorf("buffer overflow at name data")
	}
	copy(buf[index:index+nameSize], nameBytes)
	index += nameSize

	// 存储 memSpaceContentType（变长编码）
	if index+binary.MaxVarintLen64 > len(buf) {
		return nil, fmt.Errorf("buffer overflow at memSpaceContentType")
	}
	n = binary.PutVarint(buf[index:], int64(space.memSpaceContentType))
	index += n

	// 返回实际写入的字节数据
	return buf[:index], nil
}
