package raft

import (
	"ComDB/raft/pb"
	"log"
)

// operation for log entries
type raftLog struct {
	// Persistent log information
	storage   Storage
	committed uint64
	applied   uint64
	ms        *MemoryStorage
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
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	_, err = storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// Initialize our committed and applied pointers to the time of the last compaction.
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

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
	// 直接从存储中读取 lo 到 hi 的条目，不考虑 unstable 的 offset
	// todo we just read log from memory,sync module do not finished yet
	storedEnts, err := l.storage.Entries(lo, hi, maxSize)
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
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
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
		term, err := l.term(index)
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
	//
	if l.matchIndex(index, logTerm) {
		// leader and follower have the same entry in this index
		newIndex := l.ms.lastIndex() + uint64(len(msg.Entries))
		// 就这现在最新的位置往前找冲突
		conflict := l.findConflict(msg.Entries)
		switch {
		case conflict == 0:
		case conflict <= l.committed:
			log.Fatalf("entry %d conflict with committed entry [committed(%d)]\n", conflict, l.committed)
		default:
			offset := index + 1
			err := l.ms.Append(msg.Entries[conflict-offset:])
			if err != nil {
				return 0, false
			}
		}
		// update commited field in raftLog
		l.commitTo(min(msg.Commit, newIndex))
		return newIndex, true
	}
	return 0, false
}
func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			log.Fatalf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?\n", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}
func (l *raftLog) matchIndex(index uint64, term uint64) bool {
	msIndex, err := l.ms.Term(index)
	if err != nil {
		return false
	} else {
		return msIndex == term
	}
}

func (l *raftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.ms.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.ms.lastIndex() {
				log.Printf("found conflict at index %d [conflicting term: %d]\n",
					ne.Index, ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (ms *MemoryStorage) matchTerm(i, term uint64) bool {
	t, err := ms.Term(i)
	if err != nil {
		return false
	}
	return t == term
}
