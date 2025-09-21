package raft

import (
	"NucleusDB/raft/pb"
	"errors"
	_ "golang.org/x/exp/slog"
	"log"
	"sync"
)

const RaftPefix string = "raft-log-entry"

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// Storage 用异步批量写入的方式优化写入性能。外部的写入请求先由一个 goroutine 写入内存中的 WAL 缓冲区，然后由另一个 goroutine 定期将缓冲区中的日志条目批量写入磁盘
// NucleusDBStorage this part is about storage of the log entry and some related operation,
type Storage interface {
	// InitialState returns the saved HardState and ConfState information.
	InitialState() (HardState, error)
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	Entries(lo, hi uint64) ([]*pb.Entry, error)
	GetEntries() ([]*pb.Entry, error)
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() uint64
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)
	// Append entry to MemoStorage
	Append(entries []*pb.Entry) int64
	matchTerm(i, term uint64) bool
}
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex
	// only neec to save hardState
	hardState HardState
	ents      []*pb.Entry
}

func (ms *MemoryStorage) matchTerm(i, term uint64) bool {
	t, err := ms.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() (*MemoryStorage, error) {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents: make([]*pb.Entry, 0),
	}, nil
}
func (ms *MemoryStorage) GetEntries() ([]*pb.Entry, error) {
	return ms.ents, nil
}

// InitialState implements the Storage interface.
func (ms *MemoryStorage) InitialState() (HardState, error) {
	return ms.hardState, nil
}

// SetHardState saves the current HardState.
func (ms *MemoryStorage) SetHardState(st HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (ms *MemoryStorage) Entries(lo, hi uint64) ([]*pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	// remeber ents is a cache area,Index is the global pos of the Entry including the entries saved in the DB
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		log.Printf("entries' hi(%d) is out of bound lastindex(%d)\n", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	ents := ms.ents[lo-offset : hi-offset]
	return ents, nil
}

// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	if len(ms.ents) == 0 {
		return 0, nil
	}
	offset := ms.ents[0].Index
	if len(ms.ents) == 0 {
		// 任期是0就说明还没有开始投票
		return 0, nil
	}
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
func (ms *MemoryStorage) LastIndex() uint64 {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex()
}

// *ms.ents[0].Index is the log entry first pos in the global log entry
func (ms *MemoryStorage) lastIndex() uint64 {

	if len(ms.ents) == 0 {
		return 0
	}
	if ms.ents[0] == nil {
		return uint64(len(ms.ents))
	}
	return ms.ents[0].Index + uint64(len(ms.ents))
}

// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	if len(ms.ents) != 0 {
		return ms.ents[0].Index + 1
	} else {
		return 0
	}
}

// Snapshot implements the Storage interface.
//func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
//	ms.Lock()
//	defer ms.Unlock()
//	return ms.snapshot, nil
//}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
//func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
//	ms.Lock()
//	defer ms.Unlock()
//
//	//handle check for old snapshot being applied
//	msIndex := ms.snapshot.Metadata.Index
//	snapIndex := snap.Metadata.Index
//	if msIndex >= snapIndex {
//		return ErrSnapOutOfDate
//	}
//
//	ms.snapshot = snap
//	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
//	return nil
//}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
//func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
//	ms.Lock()
//	defer ms.Unlock()
//	if i <= ms.snapshot.Metadata.Index {
//		return pb.Snapshot{}, ErrSnapOutOfDate
//	}
//
//	offset := ms.ents[0].Index
//	if i > ms.lastIndex() {
//		getLogger().Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
//	}
//
//	ms.snapshot.Metadata.Index = i
//	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
//	if cs != nil {
//		ms.snapshot.Metadata.ConfState = *cs
//	}
//	ms.snapshot.Data = data
//	return ms.snapshot, nil
//}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		log.Printf("compact %d is out of bound lastindex(%d)\n", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset
	ents := make([]*pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// entries[0].Index > ms.entries[0].Index -> entries is a series of log Entry that want to save into tha WAL
func (ms *MemoryStorage) Append(entries []*pb.Entry) int64 {
	if len(entries) == 0 {
		return -1
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	// 如果append的entry完全没有在缓冲区的范围内
	if last < first {
		return -1
	}
	// 对可以放入缓冲区中的内容进行截断，这个操作是保证日志的index一定是递增的
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}
	// offset是存入日志的第一个元素与当前日志的第一个元素之间的偏移
	// 换句话来说这个是同步起点！
	var offset uint64 = 0
	if len(ms.ents) != 0 {
		offset = entries[0].Index - ms.ents[0].Index
	}

	switch {
	// 如果暂存区的数据长度大于offset，也就是现在follower的长度长于同步起点，说明有一部分的日志是需要截断的
	case uint64(len(ms.ents)) > offset:
		// 截断现在的暂存区，按照最新的日志往后拓展
		ms.ents = append([]*pb.Entry{}, ms.ents[:offset]...)
		// 将数据放入最新的暂存区
		ms.ents = append(ms.ents, entries...)
	default:
		ms.ents = append(ms.ents, entries...)
	}
	return int64(ms.lastIndex())
}
