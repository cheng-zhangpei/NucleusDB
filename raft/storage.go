package raft

import (
	"ComDB"
	"ComDB/raft/pb"
	"errors"
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
// ComDBStorage this part is about storage of the log entry and some related operation,
type Storage interface {
	// load log entry from the DB
	loadIndex() error
	// InitialState returns the saved HardState and ConfState information.
	InitialState() (HardState, pb.ConfState, error)
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)
	// sync the entries by thread (async)
	syncEntry()
}
type ComDBStorage struct {
	db *ComDB.DB
}
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex
	// only neec to save hardState
	hardState HardState
	ents      []pb.Entry
	DB        *ComDB.DB
	// 新增：缓冲区，存储未持久化的日志条目
	walBuffer []pb.Entry
	// 新增：写盘通道，通知写盘线程写盘
	commitChan chan struct{}
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() (*MemoryStorage, error) {
	defaultOptions := ComDB.DefaultOptions
	db, err := ComDB.Open(defaultOptions)
	if err != nil {
		return nil, err
	}
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		DB:         db,
		ents:       make([]pb.Entry, 1),
		walBuffer:  make([]pb.Entry, 1),
		commitChan: make(chan struct{}, 3), // 缓冲通道，避免阻塞
	}, nil
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
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	// remeber ents is a cache area,Index is the global pos of the Entry including the entries saved in the DB
	offset := ms.ents[0].Index
	if lo <= *offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		log.Println("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	ents := ms.ents[lo-*offset : hi-*offset]
	return ents, nil
}

// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < *offset {
		return 0, ErrCompacted
	}
	if int(i-*offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return *ms.ents[i-*offset].Term, nil
}

// LastIndex implements the Storage interface.
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

// *ms.ents[0].Index is the log entry first pos in the global log entry
func (ms *MemoryStorage) lastIndex() uint64 {
	return *ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return *ms.ents[0].Index + 1
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
	if compactIndex <= *offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		log.Println("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - *offset
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// entries[0].Index > ms.entries[0].Index -> entries is a series of log Entry that want to save into tha WAL
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()
	last := *entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > *entries[0].Index {
		entries = entries[first-*entries[0].Index:]
	}

	offset := *entries[0].Index - *ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		log.Println("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}

// AppendWAL the new entries to the WAL buffer.
func (ms *MemoryStorage) AppendWAL(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	// 将新条目追加到缓冲区
	ms.walBuffer = append(ms.walBuffer, entries...)
	// 通知写盘线程检查缓冲区,just give a empty struct
	ms.commitChan <- struct{}{}
	return nil
}

// syncEntry asynchronously flushes the WAL buffer to persistent storage.
func (ms *MemoryStorage) syncEntry() {
	// 启动异步写盘 goroutine
	go func() {
		for {
			select {
			// get info from channel
			case <-ms.commitChan:
				// 触发写盘
				ms.flushWAL()
			}
		}
	}()
}

// flushWAL flushes the WAL buffer to persistent storage.
func (ms *MemoryStorage) flushWAL() {
	ms.Lock()
	if len(ms.walBuffer) == 0 {
		ms.Unlock()
		return // 缓冲区为空，无需写盘
	}

	// 将缓冲区中的条目写入持久化存储（例如，写入文件或数据库）
	// 这里假设你有一个持久化方法，例如 writeToDisk
	if err := ms.writeToDisk(ms.walBuffer); err != nil {
		log.Printf("failed to flush WAL: %v", err)
	}
	// 清空缓冲区
	ms.walBuffer = []pb.Entry{}
	ms.Unlock()
}

// writeToDisk simulates writing entries to persistent storage.
// 在实际实现中，你需要根据你的存储层（如文件系统或数据库）实现 this method.
func (ms *MemoryStorage) writeToDisk(entries []pb.Entry) error {
	// todo 模拟写盘操作

	log.Printf("flushing %d entries to disk", len(entries))
	return nil
}
