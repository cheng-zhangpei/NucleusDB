package raft

import (
	"ComDB/raft/pb"
	"fmt"
	"log"
)

// operation for log entries
type raftLog struct {
	// Persistent log information
	storage   Storage
	committed uint64
	applied   uint64
	// maxNextEntsSize is the maximum number aggregate byte size of the messages
	// returned from calls to nextEnts.
	maxNextEntsSize uint64
}

func newLogWithSize(storage Storage, maxNextEntsSize uint64) *raftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &raftLog{
		storage:         storage,
		maxNextEntsSize: maxNextEntsSize,
		committed:       0,
		applied:         0,
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	//lastIndex := storage.LastIndex()
	// 这里小心一点，别溢出了，index在这个系统里面是从0开始的无符号数
	log.committed = firstIndex
	log.applied = firstIndex
	return log
}

// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
func (l *raftLog) nextEnts() (ents []*pb.Entry) {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
		if err != nil {
			log.Printf("unexpected error when getting unapplied entries")
		}
		return ents
	}
	return nil
}

func (l *raftLog) firstIndex() uint64 {
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

func (l *raftLog) slice(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []*pb.Entry

	storedEnts, err := l.storage.Entries(lo, hi)
	if err == ErrCompacted {
		return nil, err
	} else if err == ErrUnavailable {
		log.Fatalln("entries is unavailable from storage")
	} else if err != nil {
		panic(err)
	}

	// 检查条目数量是否超过了 maxSize
	if got := uint64(len(storedEnts)); got > maxSize {
		storedEnts = storedEnts[:maxSize]
	}

	ents = storedEnts
	return ents, nil
}

func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Fatalf("invalid slice %d > %d\n", int(lo), int(hi))
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.lastIndex() + 1 - fi
	if hi > fi+length {
		log.Fatalf("slice[%d,%d) out of bound [%d,%d]\n", lo, hi, fi, l.lastIndex())
	}
	return nil
}

func (l *raftLog) lastIndex() uint64 {
	i := l.storage.LastIndex()
	return i
}

func (l *raftLog) lastTerm() uint64 {
	t, err := l.storage.Term(l.lastIndex())
	if err != nil {
		log.Fatalln("unexpected error when getting the last term")
	}
	return t
}
func (l *raftLog) findConflictByTerm(index uint64, logTerm uint64) uint64 {
	if li := l.lastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		log.Printf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm\n",
			index, li)
		return index
	}
	for {
		term, err := l.storage.Term(index)
		// logterm is larger than the term. Index --
		if term <= logTerm || err != nil {
			break
		}
		index--
	}
	return index
}
func (l *raftLog) AppendWithConflictCheck(msg *pb.Message) (uint64, bool) {
	logTerm := msg.LogTerm
	index := msg.Index
	if l.matchIndex(index, logTerm) || l.isEntriesEmpty() {
		FollowerEnts, err := l.storage.GetEntries()
		if err != nil {
			panic(err)
		}
		// leader and follower have the same entry in this index
		// lastIndex, err := l.storage.LastIndex()
		newIndex := l.storage.LastIndex() + uint64(len(msg.Entries))
		// 就这现在最新的位置往前找冲突
		conflict := l.findConflict(msg.Entries)
		switch {
		case conflict == 0: // 需要直接把新的日志内容丢进去是吗？
		case conflict <= l.committed:
			log.Fatalf("entry %d conflict with committed entry [committed(%d)]\n", conflict, l.committed)
		default:
			// 此处将follower的entries给打印出来看一眼
			for _, entry := range FollowerEnts {
				fmt.Println(entry)
			}
			// 计算冲突后需要追加的日志条目起始索引
			start := max(conflict-index, 0)
			// 追加从冲突点开始的日志条目
			_ = l.storage.Append(msg.Entries[start:])
		}
		// update commited field in raftLog，这里要判断好提交信息是否合法
		l.commitTo(min(msg.Commit, newIndex))
		return newIndex, true
	}
	return 0, false
}
func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		// 所以这就是一开始不统一index的第一个元素下标的后果
		if l.lastIndex() <= tocommit {
			log.Fatalf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?\n", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}
func (l *raftLog) matchIndex(index uint64, term uint64) bool {
	msIndex, err := l.storage.Term(index)
	if err != nil {
		return false
	} else {
		return msIndex == term
	}
}

func (l *raftLog) findConflict(ents []*pb.Entry) uint64 {
	// 为了实时观察follower的变化这个地方再加上follower的logEntry的内容
	entries, err := l.storage.GetEntries()
	if err != nil {
		panic(err)
	}
	for _, entry := range entries {
		fmt.Println(entry)
	}
	for _, ne := range ents {
		if !l.storage.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.storage.LastIndex() {
				log.Printf("found conflict at index %d [conflicting term: %d]\n",
					ne.Index, ne.Term)
			}
			// ne 是来自外部的消息，一开始index肯定是0
			// 真实的index是需要小一个的
			return ne.Index - 1
		}
	}
	return 0
}
func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *raftLog) isEntriesEmpty() bool {
	entries, err := l.storage.GetEntries()
	if err != nil {
		return false
	}
	if len(entries) == 0 {
		return true
	}
	return entries[0] == nil
}
