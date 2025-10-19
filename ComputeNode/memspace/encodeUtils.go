package memspace

import (
	"encoding/binary"
)

// vector encoder
func encodeVector(memUint MemUint) *VectorRecord {
	return NewVectorRecord()

}

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

	// 存储 CreateAgentId (8字节，小端存储)
	binary.LittleEndian.PutUint64(buf[index:index+8], mm.CreateAgentId)
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
	index += binary.PutVarint(buf[index:], int64(*mm.SpaceType))

	// 存储 spaceStatus（变长编码）
	index += binary.PutVarint(buf[index:], int64(*mm.SpaceStatus))

	// 存储 spaceLimit（变长编码）
	index += binary.PutUvarint(buf[index:], mm.SpaceLimit)

	// 存储 availSpace（变长编码）
	index += binary.PutUvarint(buf[index:], mm.AvailSpace)

	// 返回实际写入的字节数据
	return buf[:index]
}
