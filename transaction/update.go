package transaction

type Update struct {
	watermark *watermark
	tracker   *tracker
}

func NewUpdate() *Update {
	watermark := newWatermark(5)
	tracker := newTracker()
	return &Update{
		watermark: watermark,
		tracker:   tracker,
	}
}
