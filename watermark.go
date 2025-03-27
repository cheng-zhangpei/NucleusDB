package ComDB

import (
	"github.com/emirpasic/gods/queues/priorityqueue"
	"log"
	"sort"
)

// 维护水位线操作，用堆来进行标识，以时间作为衡量标准来建立最小堆-> 堆顶是距离现在最久的元素,这个是最小堆

type watermark struct {
	//用于维护水位线的最大堆，
	timesHeap *priorityqueue.Queue
	//用于将堆中维护可能发生冲突的键的列表
	conflictKeys map[uint64][]byte
	// 最大堆限制大小
	maxSize uint64
	// 垃圾回收通道-> 在update中专门开一个go routine来进行过期事务回收
	gcChannel chan uint64
}

func newWatermark(maxSize uint64, db *DB) *watermark {
	pq := priorityqueue.NewWith(UInt64Comparator) // 使用自定义比较器
	ConflictKeys := make(map[uint64][]byte, 10)
	gcc := make(chan uint64, 10)
	return &watermark{
		pq,
		ConflictKeys,
		maxSize,
		gcc,
	}
}

// 无符号数比较器
func UInt64Comparator(a, b interface{}) int {
	aInt64 := a.(uint64)
	bInt64 := b.(uint64)
	switch {
	case aInt64 > bInt64:
		return 1
	case aInt64 < bInt64:
		return -1
	default:
		return 0
	}
}

func (w *watermark) addCommitTime(commitTime uint64) {
	w.timesHeap.Enqueue(commitTime)

	// 如果堆大小超过 maxSize，移除最旧元素（堆顶）
	if uint64(w.timesHeap.Size()) > w.maxSize {
		oldest, _ := w.timesHeap.Dequeue()
		oldestTime := oldest.(uint64)

		// 发送到 GC 通道（非阻塞，避免死锁）
		select {
		case w.gcChannel <- oldestTime:
		default:
			log.Println("GC channel is full, dropped timestamp:", oldestTime)
		}
	}
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

// GetTimeRange 获取所有在 [minTime, maxTime] 范围内的时间戳切片
// 注意：返回的切片按时间升序排列
func (w *watermark) GetTimeRange(minTime, maxTime uint64) []uint64 {
	if w.timesHeap.Empty() {
		return nil
	}
	// 创建临时最小堆用于恢复原堆结构
	tempHeap := priorityqueue.NewWith(UInt64Comparator)
	var result []uint64
	// 1. 遍历原堆提取符合条件的时间戳
	for !w.timesHeap.Empty() {
		current, _ := w.timesHeap.Peek()
		ts := current.(uint64)
		// 当前时间戳小于最小值，后续都大于等于（最小堆特性）
		if ts < minTime {
			break
		}
		// 出堆当前元素
		elem, _ := w.timesHeap.Dequeue()

		if ts <= maxTime {
			result = append(result, ts)
		}
		// 暂时存入临时堆
		tempHeap.Enqueue(elem)
	}

	// 2. 将临时堆元素重新放回原堆
	for !tempHeap.Empty() {
		elem, _ := tempHeap.Dequeue()
		w.timesHeap.Enqueue(elem)
	}

	// 3. 将结果排序（最小堆取出的是升序）
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	return result
}
