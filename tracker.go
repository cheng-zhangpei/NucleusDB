package ComDB

const finishedTxn = "mvcc-finished"
const MVCC_SNAPSHOT_PREFIX = "mvcc-snapshot"

// 用于监视集群中其他事务的运行状况
type tracker struct {
	// 1. conflictCheckArray 用于维护所有位于堆结构中的WriteKey
	conflictArrays map[uint64]struct{}
	// 2. 维护水位与事务之间对应的数组
	waterToTxn map[uint64]*Txn
	// 3. seqNo,这里的功能就类似数据库层面的全局锁，是需要进行key的锁编解码的
	seqNo uint64
}

func newTracker() *tracker {
	conflictArrays := make(map[uint64]struct{})
	waterToTxn := make(map[uint64]*Txn)

	return &tracker{
		conflictArrays: conflictArrays,
		waterToTxn:     waterToTxn,
		seqNo:          0,
	}
}

// todo 保存事务至数据库层中
func (tk *tracker) saveSnapshot(txn *Txn) error {

}

// todo 将返回所有的snapshot，并进行筛选最新的maxSize日志，于此同时删除旧日志也就是不在Maxsize范围内的日志
func (tk *tracker) loadAllSnapshot(maxSize uint64) error {

}

// todo 按照堆内时间戳进行日志的加载,并将数据缓存在内存中
func (tk *tracker) loadSnapshotByTime(timestamp uint64) (*Txn, error) {

}
