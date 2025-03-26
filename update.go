package ComDB

import "sync"

type Update struct {
	Watermark *watermark
	Tracker   *tracker
	UpMu      *sync.RWMutex
}

// NewUpdate 这个函数需要再数据库启动的时候就调用，保证水位的一致性
func NewUpdate() *Update {
	watermark := newWatermark(5)
	tracker := newTracker()
	return &Update{
		Watermark: watermark,
		Tracker:   tracker,
	}
}
