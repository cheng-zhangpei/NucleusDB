package ComDB

import (
	"log"
	"sync"
)

type Update struct {
	Watermark *watermark
	Tracker   *tracker
	UpMu      *sync.RWMutex
	db        *DB
}

// NewUpdate 这个函数需要再数据库启动的时候就调用，保证水位的一致性
func NewUpdate(maxSize uint64, db *DB) *Update {
	watermark := newWatermark(maxSize, db)
	tracker := newTracker()
	snapshot := loadAllSnapshot(maxSize, db)
	// 先更新水位线再更新事务映射
	for _, s := range snapshot {
		watermark.addCommitTime(s.commitTime)
		tracker.waterToTxn[s.commitTime] = s
	}
	update := &Update{
		Watermark: watermark,
		Tracker:   tracker,
		db:        db,
		UpMu:      new(sync.RWMutex),
	}
	// 启动垃圾回收线程
	go update.GC()
	return update
}

// GC 垃圾回收机制，用于回收水位线过期事务
func (update *Update) GC() {
	for {
		select {
		case timeToGC := <-update.Watermark.gcChannel:
			// 删除快照
			err := deleteSnapshotByTime(timeToGC, update.db)
			if err != nil {
				log.Println(err)
			}
			// 删除追踪数据
			update.Tracker.deleteTxn(timeToGC)
		}
	}
}

func (update *Update) Update(fn func(txn *Txn) error) ([]string, error) {
	txn := NewTxn(update.db)
	// 执行事务内部逻辑
	if err := fn(txn); err != nil {
		return nil, err
	}
	// 将事务提交,
	update.UpMu.Lock()
	ReadResult, err := txn.Commit(update.Watermark, update.Tracker)
	if err != nil {
		return nil, err
	}
	update.UpMu.Unlock()
	return ReadResult, nil
}
