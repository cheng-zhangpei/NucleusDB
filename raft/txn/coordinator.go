package txn

import (
	"errors"
	"fmt"
	"log"
	"time"
)

// 协调者: 用于协调不同节点之间的数据同步
// 职责1：负责维护分布式状态下的水位线
// 职责2：存储分布式事务快照并将内容保存在zk中
// 职责3：为分布式数据库节点的冲突检测提供时间戳

const DIS_TXN_PREFIX = "/distributed_transaction_hashKey_prefix"

type Coordinator struct {
	// 水位线管理接口与单机数据库的MVCC结构一致
	WaterMark *watermark
	// zookeeper连接
	zkAddr string
	zkConn *zookeeperConn
	// 时间戳段大小
	timeSegment uint64
}
type Operation struct {
	Cmd   string `json:"cmd"`   // 可导出
	Key   []byte `json:"key"`   // 可导出
	Value []byte `json:"value"` // 可导出
}

// TxnSnapshot 需要保存在zk中的数据

func NewCoordinator(zkAddr string) *Coordinator {
	// 开启http服务器
	zkConn := NewZookeeperConn([]string{zkAddr}, time.Second*30, nil)
	//todo 后续水位线设置为可调整
	watermark := NewWatermark(5)
	return &Coordinator{
		zkAddr:    zkAddr,
		zkConn:    zkConn,
		WaterMark: watermark,
	}
}

// handleConflictCheck 处理分布式情况下的冲突检测
func (co *Coordinator) handleConflictCheck(checkKeyList map[uint64]struct{}, startTime uint64, commitTs uint64) (bool, error) {
	// 加载所有位于水位线中的的快照
	if startTime == commitTs {
		return true, nil
	}
	timeRange := co.WaterMark.GetTimeRange(startTime, commitTs)
	// 连接zk
	log.Printf("当前水位线时间范围：")
	for timeC := range timeRange {
		log.Printf("[%d],", timeC)
	}
	log.Printf("\n")
	if !co.zkConn.connected {
		err := co.zkConn.Connect()
		if err != nil {
			return false, err
		}
	}
	// 根据时间范围从zookeeper中获取快照数据信息
	snapshots, err := co.zkConn.GetSnapshotByPrefix(DIS_TXN_PREFIX)
	if err != nil {
		return false, err
	}
	if len(snapshots) == 0 {
		return false, nil
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
		for key, _ := range txnSnapshot.PendingRepeatWrites {
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

func (co *Coordinator) saveSnapshot(txn *TxnSnapshot) error {
	if txn == nil {
		return errors.New("txn is nil")
	}
	encodeTxn := EncodeTxn(txn)
	if !co.zkConn.connected {
		err := co.zkConn.Connect()
		if err != nil {
			log.Println(err)
			return err
		}
	}
	// 暂时先添加水位线的操作
	co.WaterMark.AddCommitTime(txn.CommitTime)
	txnKey := fmt.Sprintf("%s-%d", DIS_TXN_PREFIX, txn.CommitTime)
	if _, err := co.zkConn.Set(txnKey, encodeTxn, 1); err != nil {
		log.Println(err)
		return err
	}
	return nil
}
