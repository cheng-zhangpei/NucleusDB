package transaction

// 此处暂时的设计目标是结合raft共识算法实现一个MVCC的分布式事务机制，需要先实现单机的MVCC机制并经过测试才可以做分布式事务的相关功能
/**


 */

type operation struct {
	cmd   string // 操作
	key   []byte
	value []byte
}
type Txn struct {
	// 读写区域的快照
	pendingWrite map[uint64]*operation
	// 可能存在重复写
	pendingRepeatWrite map[uint64]*operation
	pendingRead        map[uint64]*operation
	// 当前事务水位线起始时间
	startWatermark uint64
	// commit time
	commitTime uint64
}

func NewTxn() *Txn {
	// 创建水位线
	startTime := getStartTime()
	pendWrite := make(map[uint64]*operation)
	pendRepeatWrite := make(map[uint64]*operation)
	pendRead := make(map[uint64]*operation)
	return &Txn{
		pendWrite,
		pendRepeatWrite,
		pendRead,
		startTime,
		0,
	}
}

func (txn *Txn) Put(key []byte, value []byte) {
	// 将数据放入缓冲区中
	operation := &operation{
		cmd:   "PUT",
		key:   key,
		value: value,
	}
	txn.pendingWrite[txn.startWatermark] = operation

}

func (txn *Txn) Get(key []byte) {
	operation := &operation{
		cmd:   "GET",
		key:   key,
		value: []byte{},
	}
	txn.pendingRead[txn.startWatermark] = operation
}

func (txn *Txn) Delete(key []byte) {
	operation := &operation{
		cmd:   "DELETE",
		key:   key,
		value: []byte{},
	}
	txn.pendingRead[txn.startWatermark] = operation
}

// Discard 安全的删除事务中的数据，将暂存区中的数据清空
func (txn *Txn) Discard() {
	// 清空 pendingWrite
	for key := range txn.pendingWrite {
		delete(txn.pendingWrite, key)
	}
	// 清空 pendingRepeatWrite
	for key := range txn.pendingRepeatWrite {
		delete(txn.pendingRepeatWrite, key)
	}
	// 清空 pendingRead
	for key := range txn.pendingRead {
		delete(txn.pendingRead, key)
	}
	// 重置其他字段
	txn.startWatermark = 0
	txn.commitTime = 0
}

// Commit 提交数据，这个函数是事务的核心
func (txn *Txn) Commit() error {

}
