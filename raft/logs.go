package raft

// operation for log entries
type raftLog struct {
	// Persistent log information
	ComDBStorage ComDBStorage
	committed    uint64
	applied      uint64
}
