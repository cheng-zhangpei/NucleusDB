package ComDB

import (
	"encoding/binary"
)

// 此处暂时的设计目标是结合raft共识算法实现一个MVCC的分布式事务机制，需要先实现单机的MVCC机制并经过测试才可以做分布式事务的相关功能

type operation struct {
	cmd   string // PUT/GET/DELETE
	key   []byte
	value []byte
}

type Txn struct {
	// 当前最新写入操作 (key hash => operation)
	pendingWrite map[uint64]*operation
	// 同一key的多次写入历史 (仅保留前一次写入)
	pendingRepeatWrites map[uint64]*operation
	// 读操作记录 (key hash => operation)
	pendingReads map[uint64]*operation
	// 事务时间戳
	startWatermark uint64
	commitTime     uint64
	// 冲突检测用的key指纹
	conflictKeys map[uint64]struct{}
	// 统一顺序存储
	operations []*operation
	// 数据库实例
	db *DB
	// Get数据暂存区
	getResult []string
}

func NewTxn(db *DB) *Txn {
	startTime := getCurrentime()
	return &Txn{
		pendingWrite:        make(map[uint64]*operation),
		pendingRepeatWrites: make(map[uint64]*operation),
		pendingReads:        make(map[uint64]*operation),
		conflictKeys:        make(map[uint64]struct{}),
		getResult:           make([]string, 0),
		startWatermark:      startTime,
		commitTime:          0,
		db:                  db,
	}
}

// Set 写入数据
func (txn *Txn) Put(key, value []byte) error {
	hash := generateHashCode(key)

	op := &operation{
		cmd:   "PUT",
		key:   key,
		value: value,
	}
	txn.operations = append(txn.operations, op)

	// 如果已有写入，将旧值移到duplicateWrites
	if oldOp, exists := txn.pendingWrite[hash]; exists {
		txn.pendingRepeatWrites[hash] = oldOp
	}

	txn.pendingWrite[hash] = op
	return nil
}

// Get 读取数据
func (txn *Txn) Get(key []byte) error {
	hash := generateHashCode(key)
	op := &operation{
		cmd:   "GET",
		key:   key,
		value: nil,
	}
	txn.operations = append(txn.operations, op)

	txn.pendingReads[hash] = op
	return nil
}

// Delete 删除数据
func (txn *Txn) Delete(key []byte) error {
	hash := generateHashCode(key)
	op := &operation{
		cmd:   "DELETE",
		key:   key,
		value: nil,
	}
	txn.operations = append(txn.operations, op)

	// 记录冲突检测key

	// 如果已有写入，将旧值移到duplicateWrites
	if oldOp, exists := txn.pendingWrite[hash]; exists {
		txn.pendingRepeatWrites[hash] = oldOp
	}

	txn.pendingWrite[hash] = op
	return nil
}

// Discard 清空事务
func (txn *Txn) Discard() {
	txn.pendingWrite = make(map[uint64]*operation)
	txn.pendingRepeatWrites = make(map[uint64]*operation)
	txn.pendingReads = make(map[uint64]*operation)
	txn.startWatermark = 0
	txn.commitTime = 0
}

// Commit 提交数据，这个函数是事务的核心
func (txn *Txn) Commit(wm *Watermark, tk *tracker) ([]string, error) {
	// 判断事务是否合法
	if len(txn.pendingWrite) == 0 && len(txn.pendingReads) == 0 {
		return nil, ErrEmptyPending
	}
	// 注意一下事务结束之后是会释放空间的，所以tracker的数据是只能追踪现在还在运行的数据
	defer txn.Discard()
	// 提交事务
	err := txn.actualCommit(wm, tk)
	if err != nil {
		return nil, err
	}
	// 保存版本快照
	if err := saveSnapshot(txn); err != nil {
		return nil, ErrTxnSnapshotSaveFailed
	}
	if len(txn.getResult) == 0 {
		return nil, nil
	}
	return txn.getResult, nil
}

// 需要加锁保证Commit部分绝对不能被抢占
func (txn *Txn) actualCommit(wm *Watermark, tk *tracker) error {
	if txn.hasConflict(wm, tk) {
		// 暂时不提供回滚机制就直接将数据从内存中删除
		txn.Discard()
		return ErrConflict
	}
	// 运行命令，注意这里需要按照顺序进行遍历，这里的顺序通过hashcode来进行寻找
	err := txn.cmdExecutor(tk.seqNo)
	if err != nil {
		return err
	}
	// 更新tracker以及水位线
	wm.addCommitTime(txn.startWatermark)
	tk.waterToTxn[txn.startWatermark] = txn
	// 保证事务的全局递增
	tk.seqNo++
	if err = txn.db.Put([]byte(finishedTxn), Uint64ToBytesBinary(tk.seqNo, binary.BigEndian)); err != nil {
		return err
	}
	return nil

}

// 判断是否产生冲突
// todo 使用布隆过滤器快速检查冲突
// todo 如果事务的大小超过一个阈值则启动分区并行事务检查

func (txn *Txn) hasConflict(wm *Watermark, tk *tracker) bool {
	// 生成当前的时间戳
	commitedTs := getCurrentime()
	txn.commitTime = commitedTs
	// 根据事务的开始时间判断位于这个事务之间的已经提交的事务
	maybeConflictTxn := wm.GetTimeRange(txn.startWatermark, txn.commitTime)
	// 获取现在事务中可能会出现问题的Key
	conflictWriteSet := make(map[uint64]struct{})
	for key, _ := range txn.pendingReads {
		txn.conflictKeys[key] = struct{}{}
	}

	// 获取到具体的事务
	for _, txnTime := range maybeConflictTxn {
		// 快照的具体数据需要从数据库中拿取
		tempTxn, _ := loadSnapshotByTime(txnTime, txn.db)
		if _, exists := tk.waterToTxn[txnTime]; exists {
			// 收集该事务的所有写操作(包括重复写入)
			for hash := range tempTxn.pendingWrite {
				conflictWriteSet[hash] = struct{}{}
			}
			for hash := range tempTxn.pendingRepeatWrites {
				conflictWriteSet[hash] = struct{}{}
			}
		}
	}
	//
	for hash := range txn.conflictKeys {
		if _, exists := conflictWriteSet[hash]; exists {
			return true // 存在冲突
		}
	}
	return false // 无冲突
}

// 解析命令并执行
// todo 不同的事务直接被完全的隔离开来了
func (txn *Txn) cmdExecutor(seqNo uint64) error {
	for _, op := range txn.operations {
		switch op.cmd {
		case "PUT":
			//enkey := logRecordKeyWithSeq(op.key, seqNo)
			if err := txn.db.Put(op.key, op.value); err != nil {
				return err
			}
		case "GET":
			//enkey := logRecordKeyWithSeq(op.key, seqNo)
			value, err := txn.db.Get(op.key)
			if err != nil {
				return err
			} else {
				txn.getResult = append(txn.getResult, string(value))
			}
		case "DELETE":
			//enkey := logRecordKeyWithSeq(op.key, seqNo)
			if err := txn.db.Delete(op.key); err != nil {
				return err
			}
		}
	}
	return nil
}

// 添加变长编码用于进行seq的编码
// todo 串行化与MVCC不同事务的编码隔离
func (txn *Txn) logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// 解析 LogRecord 的 key，获取实际的 key 和事务序列号
func (txn *Txn) parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
