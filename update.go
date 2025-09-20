package NucleusDB

import (
	"log"
	"sync"
)

type Update struct {
	Watermark *Watermark
	Tracker   *tracker
	UpMu      *sync.RWMutex
	db        *DB
}

// NewUpdate 在数据库启动时调用，初始化水位线和事务追踪系统
func NewUpdate(maxSize uint64, db *DB) *Update {
	// 1. 初始化组件（无共享状态操作，无需加锁）
	watermark := NewWatermark(maxSize, db)
	tracker := newTracker()

	// 2. 加锁加载快照数据
	snapshot := loadAllSnapshot(maxSize, db)
	for _, s := range snapshot {
		// 原子性更新水位线和事务映射
		watermark.addCommitTime(s.commitTime)
		tracker.waterToTxn[s.commitTime] = s
	}

	// 3. 创建Update实例（此时无竞争）
	update := &Update{
		Watermark: watermark,
		Tracker:   tracker,
		db:        db,
		UpMu:      new(sync.RWMutex),
	}

	// 4. 启动GC协程（带退出机制）
	go update.GC()
	return update
}

// GC 垃圾回收机制，用于回收水位线过期事务
func (update *Update) GC() {
	for timeToGC := range update.Watermark.gcChannel { // 自动处理channel关闭
		// 加锁保护共享数据
		update.UpMu.Lock()
		err := deleteSnapshotByTime(timeToGC, update.db)
		if err != nil {
			log.Println("GC failed:", err)
		}
		update.Tracker.deleteTxn(timeToGC)
		update.UpMu.Unlock()
	}
}

// Close 在Update结构体关闭时调用
func (update *Update) Close() {
	close(update.Watermark.gcChannel)
}

func (update *Update) Update(fn func(txn *Txn) error) ([]string, error) {
	txn := NewTxn(update.db)
	if err := fn(txn); err != nil {
		return nil, err // 事务逻辑错误提前返回，不占用锁
	}

	update.UpMu.Lock() // 仅保护提交阶段
	defer update.UpMu.Unlock()

	return txn.Commit(update.Watermark, update.Tracker)
}
