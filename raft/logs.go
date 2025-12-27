package raft

import (
	"NucleusDB/raft/pb"
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
func (l *raftLog) truncateLogBeforeConflict(conflict uint64) error {
	// 1. 获取当前所有日志
	currentEntries, err := l.storage.GetEntries()
	if err != nil {
		return fmt.Errorf("failed to get current entries: %v", err)
	}

	// 2. 截断到冲突点之前
	var truncatedEntries []*pb.Entry
	if conflict-1 >= l.firstIndex() && conflict-1 <= l.lastIndex() {
		// 找到冲突点之前的所有日志
		for i := 0; i < len(currentEntries); i++ {
			if currentEntries[i].Index < conflict {
				truncatedEntries = append(truncatedEntries, currentEntries[i])
			} else {
				break
			}
		}
		log.Printf("Truncated log before conflict %d, kept %d entries", conflict, len(truncatedEntries))
	} else {
		// 如果冲突点超出范围，保留所有现有日志
		truncatedEntries = currentEntries
		log.Printf("Conflict %d out of range [%d, %d], keeping all %d entries",
			conflict, l.firstIndex(), l.lastIndex(), len(truncatedEntries))
	}

	// 3. 清空存储并重新写入截断后的日志
	if err := l.storage.Reset(truncatedEntries); err != nil {
		return fmt.Errorf("failed to reset storage: %v", err)
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
		log.Fatalf("unexpected error when getting the last term %v\n", err)
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
	// 在目前这个节点,对于这个msg本身的位置是匹配的
	if l.matchIndex(index, logTerm) || l.isEntriesEmpty() {

		// leader and follower have the same entry in this index
		// lastIndex, err := l.storage.LastIndex()
		newIndex := l.storage.LastIndex() + uint64(len(msg.Entries))
		// 就这现在最新的位置往前找冲突
		conflict := l.findConflict(msg.Entries)
		// 这很恐怖,冲突点已经提交了,我已经没法在缓冲区中去改变数据了
		if conflict > 0 && conflict <= l.committed {
			return 0, false
		}

		switch {
		case conflict == 0:
			// 不存在冲突就直接放入
			_ = l.storage.Append(msg.Entries[:])
		case conflict <= l.committed:
			log.Fatalf("entry %d conflict with committed entry [committed(%d)]\n", conflict, l.committed)
		default:
			log.Printf("node follower start process conflict at index %d!\n", conflict)
			// 对当前的日志数据进行截断
			if err := l.truncateLogBeforeConflict(conflict); err != nil {
				log.Printf("Failed to truncate log before conflict: %v", err)
				return 0, false
			}
			// 在截断数据之后去添加
			start := max(conflict-index, 0)
			_ = l.storage.Append(msg.Entries[start:])

			// 调试输出
			//FollowerEnts, err := l.storage.GetEntries()
			//if err != nil {
			//	panic(err)
			//}
			log.Printf("After conflict resolution - entries:")
			//for _, entry := range FollowerEnts {
			//log.Printf("Index: %d, Term: %d", entry.Index, entry.Term)
			//}
		}
		// update commited field in raftLog，这里要判断好提交信息是否合法
		l.commitTo(min(msg.Commit, newIndex))
		return newIndex, true
	}

	return 0, false
}

// 这个函数的意思是我所有节点的即将commit的Index是不能超过缓冲区的大小的，否则就溢出了
func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {

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
			return ne.Index
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

// 安全阀，也就是规则
// Rules:
//   - If the candidate's last entry term is greater than ours, it is up-to-date.
//   - If the terms are equal, and the candidate's last entry index is >= ours, it is up-to-date.
//   - Otherwise, it is not up-to-date.
func (l *raftLog) isUpToDate(index uint64, term uint64) bool {
	// Get the index and term of the last entry in our own log.
	myLastIndex := l.lastIndex()
	myLastTerm := l.lastTerm()
	// Rule 1: If the candidate's LogTerm is higher, its log is more up-to-date.
	if term > myLastTerm {
		return true
	}
	// Rule 2: If the LogTerms are equal, we compare the indices.
	//         The candidate's log is more up-to-date if its index is greater than or equal to ours.
	if term == myLastTerm && index >= myLastIndex {
		return true
	}
	// In all other cases (term < myLastTerm, or term==myLastTerm but index < myLastIndex),
	// the candidate's log is not up-to-date.
	return false
}
