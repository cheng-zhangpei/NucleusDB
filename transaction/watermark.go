package transaction

import (
	"github.com/emirpasic/gods/queues/priorityqueue"
)

// 维护水位线操作，用堆来进行标识，以时间作为衡量标准来建立最小堆-> 堆顶是距离现在最久的元素

type watermark struct {
	//用于维护水位线的最大堆，
	timesHeap *priorityqueue.Queue
	//用于将堆中维护可能发生冲突的键的列表
	conflictKeys map[uint64][]byte
	// 最大堆限制大小
	maxSize uint64
}

func newWatermark(maxSize uint64) *watermark {
	pq := priorityqueue.NewWith(UInt64Comparator) // 使用自定义比较器
	ConflictKeys := make(map[uint64][]byte, 10)
	return &watermark{
		pq,
		ConflictKeys,
		maxSize,
	}

}

// 无符号数比较器
func UInt64Comparator(a, b interface{}) int {
	aInt64 := a.(uint64)
	bInt64 := b.(uint64)
	switch {
	case aInt64 < bInt64:
		return 1
	case aInt64 > bInt64:
		return -1
	default:
		return 0
	}
}

// 添加事务
func (w *watermark) addCommitTime(commitTime uint64) {
	w.timesHeap.Enqueue(commitTime)
}

func (w *watermark) getLatestCommitTime() uint64 {
	if w.timesHeap.Empty() {
		return 0 // 如果堆为空，返回 0
	}
	value, _ := w.timesHeap.Peek()
	return value.(uint64)
}
func (w *watermark) removeLatestCommitTime() {
	if !w.timesHeap.Empty() {
		w.timesHeap.Dequeue()
	}
}
