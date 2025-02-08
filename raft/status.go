package raft

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
type BasicStatus struct {
	ID uint64

	HardState
	SoftState

	Applied uint64

	LeadTransferee uint64
}
