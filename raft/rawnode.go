package raft

import (
	"ComDB"
	"ComDB/raft/pb"
	"errors"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")
var emptyState = HardState{}

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.

type RawNode struct {
	raft       *raft
	prevSoftSt *SoftState
	prevHardSt *HardState
}
type Peer struct {
	ID      uint64
	Context []byte
}

// NewRawNode instantiates a RawNode from the given configuration.
//
// See Bootstrap() for bootstrapping an initial state; this replaces the former
// 'peers' argument to this method (with identical behavior). However, It is
// recommended that instead of calling Bootstrap, applications bootstrap their
// state manually by setting up a Storage that has a first index > 1 and which
// stores the desired ConfState as its InitialState.
func NewRawNode(config *RaftConfig, options ComDB.Options) (*RawNode, error) {
	r := newRaft(config, options)
	rn := &RawNode{
		raft: r,
	}
	rn.prevSoftSt = r.softState()
	rn.prevHardSt = r.hardState()
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.raft.tick()
}
func (rn *RawNode) TickQuiesced() {
	rn.raft.electionElapsed++
}

// Campaign try to become leader
func (rn *RawNode) Campaign() error {
	return rn.raft.Step(&pb.Message{
		Type: pb.MessageType_MsgHup,
	})
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(msg *pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(msg.Type) {
		return ErrStepLocalMsg
	}
	// put request to the raft structure
	if pr := rn.raft.processTracker.Progress[msg.From]; pr != nil || !IsResponseMsg(msg.Type) {
		return rn.raft.Step(msg)
	}
	return ErrStepPeerNotFound
}

// Ready returns the outstanding work that the application needs to handle. This
// includes appending and applying entries or a snapshot, updating the HardState,
// and sending messages. The returned Ready() *must* be handled and subsequently
// passed back via Advance().
func (rn *RawNode) Ready() *Ready {
	rd := rn.readyWithoutAccept()
	rn.acceptReady(rd)
	// 到这个位置我知道上层的应用已经把事情给做完了
	return rd
}

// readyWithoutAccept returns a Ready. This is a read-only operation, i.e. there
// is no obligation that the Ready must be handled.
func (rn *RawNode) readyWithoutAccept() *Ready {
	return newReady(rn.raft, rn.prevSoftSt, rn.prevHardSt)
}

// acceptReady is called when the consumer of the RawNode has decided to go
// ahead and handle a Ready. Nothing must alter the state of the RawNode between
// this call and the prior call to Ready().
func (rn *RawNode) acceptReady(rd *Ready) {
	if &rd.SoftState != nil {
		rn.prevSoftSt = &rd.SoftState
	}
	rn.raft.msgs = nil
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
//func (rn *RawNode) Advance(rd *Ready) {
//	if !IsEmptyHardState(&rd.HardState) {
//		rn.prevHardSt = &rd.HardState
//	}
//	rn.raft.advance(rd)
//}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st *HardState) bool {
	return isHardStateEqual(st, &emptyState)
}

// HasReady called when RawNode user need to check if any Ready pending.
// Checking logic in this method should be consistent with Ready.containsUpdates().
func (rn *RawNode) HasReady() bool {
	r := rn.raft
	if !r.softState().equal(rn.prevSoftSt) {
		// has changes
		return true
	}
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rn.prevHardSt) {
		return true
	}
	if len(r.msgs) > 0 || len(r.ms.ents) > 0 || len(r.raftLog.nextEnts()) > 0 {
		return true
	}
	return false
}

func (rn *RawNode) Bootstrap(peers []Peer) error {
	if len(peers) == 0 {
		return errors.New("must provide at least one peer to Bootstrap")
	}
	lastIndex := rn.raft.raftLog.storage.LastIndex()

	if lastIndex != 0 {
		return errors.New("can't bootstrap a nonempty Storage")
	}
	rn.prevHardSt = &emptyState
	rn.raft.becomeFollower(1, None)
	return nil
}
