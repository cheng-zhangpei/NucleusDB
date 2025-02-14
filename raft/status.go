package raft

import "ComDB/raft/tracker"

// HardStatus the message that should be saved in db
type HardStatus struct {
}

// SoftStatus some status can get from other node
type SoftStatus struct {
}
type HardState struct {
	Term   uint64 `protobuf:"varint,1,opt,name=term" json:"term"`
	Vote   uint64 `protobuf:"varint,2,opt,name=vote" json:"vote"`
	Commit uint64 `protobuf:"varint,3,opt,name=commit" json:"commit"`
}
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}
type BaseStatus struct {
	ID uint64

	HardState
	SoftState
	Progress map[uint64]tracker.Progress

	Applied uint64
}

func getProgressCopy(r *raft) map[uint64]tracker.Progress {
	m := make(map[uint64]tracker.Progress)
	r.processTracker.Visit(func(id uint64, pr *tracker.Progress) {
		p := *pr
		//p.Inflights = pr.Inflights.Clone()
		pr = nil

		m[id] = p
	})
	return m
}

func getBasicStatus(r *raft) *BaseStatus {
	s := &BaseStatus{
		ID: r.id,
	}
	s.HardState = *r.hardState()
	s.SoftState = *r.softState()
	s.Applied = r.raftLog.applied
	return s
}

// getStatus gets a copy of the current raft status.
func getStatus(r *raft) *BaseStatus {
	var s *BaseStatus
	s = getBasicStatus(r)
	if s.RaftState == StateLeader {
		s.Progress = getProgressCopy(r)
	}
	return s
}
func isHardStateEqual(a, b *HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}
func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}
