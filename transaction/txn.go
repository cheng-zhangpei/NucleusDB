package transaction

// 此处暂时的设计目标是结合raft共识算法实现一个MVCC的分布式事务机制，需要先实现单机的MVCC机制并经过测试才可以做分布式事务的相关功能
/**


 */

type operation struct {
	cmd   string // 操作
	key   string
	value []byte
}
type txn struct {
	// 读写区域的快照
	pendingWrite map[uint64]operation
	// 可能存在重复写
	pendingRepeatWrite map[uint64]operation
	pendingRead        map[uint64]operation
	// 当前事务水位线起始时间
	startWatermark uint64
	// commit time
	commitTime uint64
}

func newTxn() *txn {
	// 创建水位线
	startTime := getStartTime()
	pendWrite := make(map[uint64]operation)
	pendRepeatWrite := make(map[uint64]operation)
	pendRead := make(map[uint64]operation)
	return &txn{
		pendWrite,
		pendRepeatWrite,
		pendRead,
		startTime,
		0,
	}
}

func (txn *txn) Put(key []byte, value []byte) {
	// 将数据放入缓冲区中
}

func (txn *txn) Get(key []byte) []byte {

}

func (txn *txn) Delete(key []byte) {

}
