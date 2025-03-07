package transaction

import "ComDB"

type Update struct {
	watermark *watermark
	tracker   *tracker
}

func NewUpdate() *Update {
	watermark := newWatermark(ComDB.DefaultOptions.MaxWaterSize)
	tracker := newTracker()
	return &Update{
		watermark: watermark,
		tracker:   tracker,
	}
}

// 提交数据
func (up *Update) commit(txn *txn) {
	// 1. 生成当前的CommitTime
	commitedTime := getStartTime()
	// 2. 检查是否发生冲突
	pendingRead := make([]uint64, len(txn.pendingRead))
	for key, _ := range txn.pendingRead {
		pendingRead = append(pendingRead, key)
	}
	up.tracker.checkConflict(pendingRead)
}
