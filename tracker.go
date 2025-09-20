package NucleusDB

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
func (tk *tracker) deleteTxn(timeToDelete uint64) {
	delete(tk.waterToTxn, timeToDelete)
}
