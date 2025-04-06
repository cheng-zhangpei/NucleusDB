package txn

type TxnSnapshot struct {
	// 当前最新写入操作 (key hash => Operation)
	PendingWrite map[uint64]*Operation
	// 同一key的多次写入历史 (仅保留前一次写入)
	pendingRepeatWrites map[uint64]*Operation
	// 读操作记录 (key hash => operation)
	PendingReads map[uint64]*Operation
	// 事务时间戳
	StartWatermark uint64
	commitTime     uint64
	// 冲突检测用的key指纹
	ConflictKeys map[uint64]struct{}
	// 统一顺序存储
	Operations []*Operation
}

// Set 写入数据
func (txn *TxnSnapshot) Put(key, value []byte) error {
	hash := generateHashCode(key)

	op := &Operation{
		Cmd:   "PUT",
		Key:   key,
		Value: value,
	}
	txn.Operations = append(txn.Operations, op)

	// 如果已有写入，将旧值移到duplicateWrites
	if oldOp, exists := txn.PendingWrite[hash]; exists {
		txn.pendingRepeatWrites[hash] = oldOp
	}

	txn.PendingWrite[hash] = op
	return nil
}

// Get 读取数据
func (txn *TxnSnapshot) Get(key []byte) error {
	hash := generateHashCode(key)
	op := &Operation{
		Cmd:   "GET",
		Key:   key,
		Value: nil,
	}
	txn.Operations = append(txn.Operations, op)

	txn.PendingReads[hash] = op
	return nil
}

// Delete 删除数据
func (txn *TxnSnapshot) Delete(key []byte) error {
	hash := generateHashCode(key)
	op := &Operation{
		Cmd:   "DELETE",
		Key:   key,
		Value: nil,
	}
	txn.Operations = append(txn.Operations, op)

	// 记录冲突检测key

	// 如果已有写入，将旧值移到duplicateWrites
	if oldOp, exists := txn.PendingWrite[hash]; exists {
		txn.pendingRepeatWrites[hash] = oldOp
	}

	txn.PendingWrite[hash] = op
	return nil
}

// Discard 清空事务
func (txn *TxnSnapshot) Discard() {
	txn.PendingWrite = make(map[uint64]*Operation)
	txn.pendingRepeatWrites = make(map[uint64]*Operation)
	txn.PendingReads = make(map[uint64]*Operation)
	txn.StartWatermark = 0
	txn.commitTime = 0
}
