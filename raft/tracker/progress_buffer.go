package tracker

// Inflights uncertain message buffer: Inflights（环形缓冲区）是 Progress 中的一部分，用于管理 Leader 向 Follower 发送但尚未确认的日志条目（未确认消息）
type Inflights struct {
	// the starting index in the buffer
	start int
	// number of inflights in the buffer
	count int

	// the size of the buffer
	size int

	// buffer contains the index of the last entry
	// inside one message.
	buffer []uint64
}

// Full returns true if no more messages can be sent at the moment.
func (in *Inflights) Full() bool {
	return in.count == in.size
}

// Count returns the number of inflight messages.
func (in *Inflights) Count() int { return in.count }

// reset frees all inflights.
func (in *Inflights) reset() {
	in.count = 0
	in.start = 0
}
func (in *Inflights) Add(inflight uint64) {
	// 检查未确认消息队列是否已满
	if in.Full() {
		panic("无法向已满的未确认消息队列添加新消息")
	}
	// 计算下一个未确认消息在缓冲区中的位置
	next := in.start + in.count
	size := in.size
	if next >= size {
		// 如果超出缓冲区大小，使用模运算调整位置
		next -= size
	}
	// 如果缓冲区长度不足，动态扩展缓冲区
	if next >= len(in.buffer) {
		in.grow()
	}
	// 将新的未确认消息索引添加到缓冲区
	in.buffer[next] = inflight
	in.count++ // 增加未确认消息的数量
}
func (in *Inflights) grow() {
	// 计算新缓冲区的大小
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1 // 如果当前缓冲区为空，初始化为大小 1
	} else if newSize > in.size {
		newSize = in.size // 限制新缓冲区大小不超过最大配置值
	}
	// 创建一个新的缓冲区，并将旧缓冲区的内容复制过来
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	// 更新缓冲区为新的缓冲区
	in.buffer = newBuffer
}

func (in *Inflights) FreeFirstOne() { in.FreeLE(in.buffer[in.start]) }

// FreeLE frees the inflights smaller or equal to the given `to` flight.
func (in *Inflights) FreeLE(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {
		// out of the left side of the window
		return
	}

	idx := in.start
	var i int
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx] { // found the first large inflight
			break
		}

		// increase index and maybe rotate
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i inflights and set new start index
	in.count -= i
	in.start = idx
	if in.count == 0 {
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}
