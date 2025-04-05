package txn

import (
	"fmt"
	"time"
)

// 协调者: 用于协调不同节点之间的数据同步
// 职责1：负责维护分布式状态下的水位线
// 职责2：存储分布式事务快照并将内容保存在zk中
// 职责3：为分布式数据库节点的冲突检测提供时间戳

const DIS_TXN_PREFIX = "/distributed_transaction_hashKey_prefix"

type Coordinator struct {
	// 水位线管理接口与单机数据库的MVCC结构一致
	waterMark *watermark
	// zookeeper连接
	zkAddr string
	zkConn *zookeeperConn
	// 时间戳段大小
	timeSegment uint64
}
type Operation struct {
	Cmd   string // PUT/GET/DELETE
	Key   []byte
	Value []byte
}

// TxnSnapshot 需要保存在zk中的数据

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

func NewCoordinator(zkAddr string) *Coordinator {
	// 开启http服务器
	zkConn := NewZookeeperConn([]string{zkAddr}, time.Second*5, nil)
	watermark := NewWatermark(5)
	return &Coordinator{
		zkAddr:    zkAddr,
		zkConn:    zkConn,
		waterMark: watermark,
	}
}
func NewTxnSnapshot() *TxnSnapshot {
	return &TxnSnapshot{
		PendingWrite:        make(map[uint64]*Operation),
		pendingRepeatWrites: make(map[uint64]*Operation),
		PendingReads:        make(map[uint64]*Operation),
		// 初始化水位线设置为0
		StartWatermark: 0,
		commitTime:     0,
		ConflictKeys:   make(map[uint64]struct{}),
		Operations:     make([]*Operation, 0),
	}
}

// handleConflictCheck 处理分布式情况下的冲突检测
func (co *Coordinator) handleConflictCheck(checkKeyList map[uint64]struct{}, startTime uint64, commitTs uint64) (bool, error) {
	// 加载所有位于水位线中的的快照
	timeRange := co.waterMark.GetTimeRange(startTime, commitTs)

	// 根据时间范围从zookeeper中获取快照数据信息
	snapshots, err := co.zkConn.GetSnapshotByPrefix(DIS_TXN_PREFIX)
	if err != nil {
		return false, err
	}
	ConflictKeysSet := make(map[uint64]struct{})
	// 收集在这个区间中所出现的所有的key，或者说已经被修改的key，这里不需要担心覆盖，只要出现就是有冲突
	for _, time := range timeRange {
		enSnapshot := snapshots[time]
		txnSnapshot := DecodeTxn(enSnapshot)
		// 判断是否与该事务冲突
		for key, _ := range txnSnapshot.PendingWrite {
			ConflictKeysSet[key] = struct{}{}
		}
		for key, _ := range txnSnapshot.pendingRepeatWrites {
			ConflictKeysSet[key] = struct{}{}
		}
	}
	for hash := range checkKeyList {
		if _, exists := ConflictKeysSet[hash]; exists {
			return true, nil // 存在冲突
		}
	}
	// 不存在冲突，返回false
	return false, nil

}

func (co *Coordinator) saveSnapshot(txn *TxnSnapshot, zkConn *zookeeperConn) error {
	encodeTxn := EncodeTxn(txn)
	txnKey := fmt.Sprintf("%s-%d", DIS_TXN_PREFIX, txn.commitTime)
	if _, err := zkConn.Set(txnKey, encodeTxn, 1); err != nil {
		return err
	}
	return nil
}
