package txn

import "log"

type TxnSnapshot struct {
	PendingWrite        map[uint64]*Operation `json:"pendingWrite"`
	PendingRepeatWrites map[uint64]*Operation `json:"pendingRepeatWrites"` // 改为大写
	PendingReads        map[uint64]*Operation `json:"pendingReads"`
	StartWatermark      uint64                `json:"startWatermark"`
	CommitTime          uint64                `json:"commitTime"`
	ConflictKeys        map[uint64]struct{}   `json:"conflictKeys"`
	Operations          []*Operation          `json:"operations"`
}

func NewTxnSnapshot() *TxnSnapshot {
	return &TxnSnapshot{
		PendingWrite:        make(map[uint64]*Operation),
		PendingRepeatWrites: make(map[uint64]*Operation),
		PendingReads:        make(map[uint64]*Operation),
		// 初始化水位线设置为0
		StartWatermark: 0,
		CommitTime:     0,
		ConflictKeys:   make(map[uint64]struct{}),
		Operations:     make([]*Operation, 0),
	}
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
		txn.PendingRepeatWrites[hash] = oldOp
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
	log.Println("已经构造delete")
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
		txn.PendingRepeatWrites[hash] = oldOp
	}

	txn.PendingWrite[hash] = op
	return nil
}

// Discard 清空事务
func (txn *TxnSnapshot) Discard() {
	txn.PendingWrite = make(map[uint64]*Operation)
	txn.PendingRepeatWrites = make(map[uint64]*Operation)
	txn.PendingReads = make(map[uint64]*Operation)
	txn.StartWatermark = 0
	txn.CommitTime = 0
}
