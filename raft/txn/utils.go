package txn

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"time"
)

// EncodeTxn 编码入口函数
func EncodeTxn(txn *TxnSnapshot) []byte {
	buf := make([]byte, 16) // 初始分配16字节（足够存放头信息）
	var index int

	// 时间戳（固定长度）
	binary.LittleEndian.PutUint64(buf[index:], txn.StartWatermark)
	index += 8
	binary.LittleEndian.PutUint64(buf[index:], txn.CommitTime)
	index += 8

	// 编码各字段（顺序必须与解码保持一致）
	buf = encodeMap(buf, &index, txn.PendingWrite)
	buf = encodeMap(buf, &index, txn.pendingRepeatWrites)
	buf = encodeMap(buf, &index, txn.PendingReads)
	buf = encodeConflictKeys(buf, &index, txn.ConflictKeys)
	buf = encodeOperations(buf, &index, txn.Operations)

	return buf[:index]
}
func GenerateKeyHashCode(key []byte) uint64 {
	h := fnv.New64a()
	_, err := h.Write(key)
	if err != nil {
		return 0
	}
	return h.Sum64()
}

// 辅助编码方法
func encodeMap(buf []byte, index *int, m map[uint64]*Operation) []byte {
	// 写入map长度
	buf = putVarintWithExtend(buf, index, int64(len(m)))

	for key, op := range m {
		// 写入key（固定8字节）
		buf = extendBuf(buf, *index+8)
		binary.LittleEndian.PutUint64(buf[*index:], key)
		*index += 8

		// 递归编码operation
		buf = encodeOperation(buf, index, op)
	}
	return buf
}

func encodeOperation(buf []byte, index *int, op *Operation) []byte {
	// 命令类型（1字节）
	buf = extendBuf(buf, *index+1)
	switch op.Cmd {
	case "PUT":
		buf[*index] = 1
	case "GET":
		buf[*index] = 2
	case "DELETE":
		buf[*index] = 3
	}
	*index++

	// 变长编码key
	buf = putVarintWithExtend(buf, index, int64(len(op.Key)))
	buf = extendBuf(buf, *index+len(op.Key))
	copy(buf[*index:], op.Key)
	*index += len(op.Key)

	// 仅PUT需要value
	if op.Cmd == "PUT" {
		buf = putVarintWithExtend(buf, index, int64(len(op.Value)))
		buf = extendBuf(buf, *index+len(op.Value))
		copy(buf[*index:], op.Value)
		*index += len(op.Value)
	}
	return buf
}

func encodeConflictKeys(buf []byte, index *int, m map[uint64]struct{}) []byte {
	// 写入长度
	buf = putVarintWithExtend(buf, index, int64(len(m)))

	// 写入每个key（固定8字节）
	buf = extendBuf(buf, *index+8*len(m))
	for k := range m {
		binary.LittleEndian.PutUint64(buf[*index:], k)
		*index += 8
	}
	return buf
}

func encodeOperations(buf []byte, index *int, ops []*Operation) []byte {
	// 写入切片长度
	buf = putVarintWithExtend(buf, index, int64(len(ops)))

	// 逐个编码operation
	for _, op := range ops {
		buf = encodeOperation(buf, index, op)
	}
	return buf
}

func encodeStrings(buf []byte, index *int, strs []string) []byte {
	// 写入切片长度
	buf = putVarintWithExtend(buf, index, int64(len(strs)))

	// 逐个编码字符串
	for _, s := range strs {
		buf = putVarintWithExtend(buf, index, int64(len(s)))
		buf = extendBuf(buf, *index+len(s))
		copy(buf[*index:], []byte(s))
		*index += len(s)
	}
	return buf
}

// 解码逻辑
func DecodeTxn(data []byte) *TxnSnapshot {
	txn := &TxnSnapshot{
		PendingWrite:        make(map[uint64]*Operation),
		pendingRepeatWrites: make(map[uint64]*Operation),
		PendingReads:        make(map[uint64]*Operation),
		ConflictKeys:        make(map[uint64]struct{}),
	}

	var index int

	// 解码时间戳
	txn.StartWatermark = binary.LittleEndian.Uint64(data[index:])
	index += 8
	txn.CommitTime = binary.LittleEndian.Uint64(data[index:])
	index += 8

	// 按编码顺序解码各字段
	txn.PendingWrite = decodeMap(data, &index)
	txn.pendingRepeatWrites = decodeMap(data, &index)
	txn.PendingReads = decodeMap(data, &index)
	txn.ConflictKeys = decodeConflictKeys(data, &index)
	txn.Operations = decodeOperations(data, &index)
	return txn
}

// 解码辅助方法
func decodeMap(data []byte, index *int) map[uint64]*Operation {
	size, n := binary.Varint(data[*index:])
	*index += n

	m := make(map[uint64]*Operation, size)
	for i := int64(0); i < size; i++ {
		// 解码key
		key := binary.LittleEndian.Uint64(data[*index:])
		*index += 8

		// 解码operation
		op := decodeOperation(data, index)
		m[key] = op
	}
	return m
}

func decodeOperation(data []byte, index *int) *Operation {
	op := &Operation{}

	// 解码命令类型
	switch data[*index] {
	case 1:
		op.Cmd = "PUT"
	case 2:
		op.Cmd = "GET"
	case 3:
		op.Cmd = "DELETE"
	}
	*index++

	// 解码key
	keyLen, n := binary.Varint(data[*index:])
	*index += n
	op.Key = make([]byte, keyLen)
	copy(op.Key, data[*index:*index+int(keyLen)])
	*index += int(keyLen)

	// 解码value（仅PUT）
	if op.Cmd == "PUT" {
		valLen, n := binary.Varint(data[*index:])
		*index += n
		op.Value = make([]byte, valLen)
		copy(op.Value, data[*index:*index+int(valLen)])
		*index += int(valLen)
	}

	return op
}
func GetCurrenTime() uint64 {
	timestampSec := uint64(time.Now().Unix())
	return timestampSec
}
func decodeConflictKeys(data []byte, index *int) map[uint64]struct{} {
	size, n := binary.Varint(data[*index:])
	*index += n

	m := make(map[uint64]struct{}, size)
	for i := int64(0); i < size; i++ {
		k := binary.LittleEndian.Uint64(data[*index:])
		*index += 8
		m[k] = struct{}{}
	}
	return m
}

func decodeOperations(data []byte, index *int) []*Operation {
	size, n := binary.Varint(data[*index:])
	*index += n

	ops := make([]*Operation, size)
	for i := range ops {
		ops[i] = decodeOperation(data, index)
	}
	return ops
}

func decodeStrings(data []byte, index *int) []string {
	size, n := binary.Varint(data[*index:])
	*index += n

	strs := make([]string, size)
	for i := range strs {
		strLen, n := binary.Varint(data[*index:])
		*index += n
		strs[i] = string(data[*index : *index+int(strLen)])
		*index += int(strLen)
	}
	return strs
}

// 工具方法
func putVarintWithExtend(buf []byte, index *int, val int64) []byte {
	buf = extendBuf(buf, *index+binary.MaxVarintLen64)
	n := binary.PutVarint(buf[*index:], val)
	*index += n
	return buf
}

func extendBuf(buf []byte, need int) []byte {
	if need <= cap(buf) {
		return buf
	}
	newBuf := make([]byte, need)
	copy(newBuf, buf)
	return newBuf
}

// // 这个函数主要是数据库重启的时候重启MVCC事务机制
// func loadAllSnapshot(maxSize uint64) []*TxnSnapshot {
//
// }
//
// // loadSnapshotByTime 根据时间戳加载对应的事务快照
// // 参数 timestamp 是要查找的事务提交时间
// // 返回找到的事务快照指针，如果没有找到则返回错误
// func loadSnapshotByTime(timestamp uint64, db *DB) (*Txn, error) {
//
// }
//
// func deleteSnapshotByTime(timestamp uint64, db *DB) error {
//
// }
func generateHashCode(key []byte) uint64 {
	h := fnv.New64a()
	_, err := h.Write(key)
	if err != nil {
		return 0
	}
	return h.Sum64()
}

const (
	LogicalBits  = 16 // 逻辑时钟占用位数
	PhysicalBits = 48 // 物理时钟占用位数
	MaxLogical   = (1 << LogicalBits) - 1
)

// GenerateHybridTs 生成混合时间戳（高16位逻辑时钟 + 低48位物理时钟）
func GenerateHybridTs(logicalTs uint64) (uint64, error) {
	if logicalTs > MaxLogical {
		return 0, fmt.Errorf("逻辑时钟超出范围(%d > %d)", logicalTs, MaxLogical)
	}
	// 获取物理时间（微秒级）
	now := time.Now()
	micros := now.UnixMicro()                           // 微秒级时间戳
	nanos := now.Nanosecond() % 1000                    // 纳秒部分
	physicalTs := uint64(micros<<10) | uint64(nanos>>3) // 48位物理时间戳
	// 组合时间戳
	return (logicalTs << PhysicalBits) | (physicalTs & 0xFFFFFFFFFFFF), nil
}
