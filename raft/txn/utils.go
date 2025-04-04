package txn

import (
	"encoding/binary"
	"hash/fnv"
)

// EncodeTxn 编码入口函数
func EncodeTxn(txn *TxnSnapshot) []byte {
	buf := make([]byte, 16) // 初始分配16字节（足够存放头信息）
	var index int

	// 时间戳（固定长度）
	binary.LittleEndian.PutUint64(buf[index:], txn.startWatermark)
	index += 8
	binary.LittleEndian.PutUint64(buf[index:], txn.commitTime)
	index += 8

	// 编码各字段（顺序必须与解码保持一致）
	buf = encodeMap(buf, &index, txn.pendingWrite)
	buf = encodeMap(buf, &index, txn.pendingRepeatWrites)
	buf = encodeMap(buf, &index, txn.pendingReads)
	buf = encodeConflictKeys(buf, &index, txn.conflictKeys)
	buf = encodeOperations(buf, &index, txn.operations)

	return buf[:index]
}

// 辅助编码方法
func encodeMap(buf []byte, index *int, m map[uint64]*operation) []byte {
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

func encodeOperation(buf []byte, index *int, op *operation) []byte {
	// 命令类型（1字节）
	buf = extendBuf(buf, *index+1)
	switch op.cmd {
	case "PUT":
		buf[*index] = 1
	case "GET":
		buf[*index] = 2
	case "DELETE":
		buf[*index] = 3
	}
	*index++

	// 变长编码key
	buf = putVarintWithExtend(buf, index, int64(len(op.key)))
	buf = extendBuf(buf, *index+len(op.key))
	copy(buf[*index:], op.key)
	*index += len(op.key)

	// 仅PUT需要value
	if op.cmd == "PUT" {
		buf = putVarintWithExtend(buf, index, int64(len(op.value)))
		buf = extendBuf(buf, *index+len(op.value))
		copy(buf[*index:], op.value)
		*index += len(op.value)
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

func encodeOperations(buf []byte, index *int, ops []*operation) []byte {
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
		pendingWrite:        make(map[uint64]*operation),
		pendingRepeatWrites: make(map[uint64]*operation),
		pendingReads:        make(map[uint64]*operation),
		conflictKeys:        make(map[uint64]struct{}),
	}

	var index int

	// 解码时间戳
	txn.startWatermark = binary.LittleEndian.Uint64(data[index:])
	index += 8
	txn.commitTime = binary.LittleEndian.Uint64(data[index:])
	index += 8

	// 按编码顺序解码各字段
	txn.pendingWrite = decodeMap(data, &index)
	txn.pendingRepeatWrites = decodeMap(data, &index)
	txn.pendingReads = decodeMap(data, &index)
	txn.conflictKeys = decodeConflictKeys(data, &index)
	txn.operations = decodeOperations(data, &index)
	return txn
}

// 解码辅助方法
func decodeMap(data []byte, index *int) map[uint64]*operation {
	size, n := binary.Varint(data[*index:])
	*index += n

	m := make(map[uint64]*operation, size)
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

func decodeOperation(data []byte, index *int) *operation {
	op := &operation{}

	// 解码命令类型
	switch data[*index] {
	case 1:
		op.cmd = "PUT"
	case 2:
		op.cmd = "GET"
	case 3:
		op.cmd = "DELETE"
	}
	*index++

	// 解码key
	keyLen, n := binary.Varint(data[*index:])
	*index += n
	op.key = make([]byte, keyLen)
	copy(op.key, data[*index:*index+int(keyLen)])
	*index += int(keyLen)

	// 解码value（仅PUT）
	if op.cmd == "PUT" {
		valLen, n := binary.Varint(data[*index:])
		*index += n
		op.value = make([]byte, valLen)
		copy(op.value, data[*index:*index+int(valLen)])
		*index += int(valLen)
	}

	return op
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

func decodeOperations(data []byte, index *int) []*operation {
	size, n := binary.Varint(data[*index:])
	*index += n

	ops := make([]*operation, size)
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
