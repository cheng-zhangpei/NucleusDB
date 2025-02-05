package raft

import (
	"errors"
	"math"
)

const None uint64 = 0
const noLimit = math.MaxUint64

type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// raft consensus algorithm: build a simple distributed database in order to adapt the env of k8s
type RaftConfig struct {
	ID uint64
	// ComDBStorage a storage struct to manage the status and log entries status
	ComDBStorage ComDBStorage
	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick uint64
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick uint64
	// MaxSizePerMsg limit the size of byte message that user can submit
	MaxSizePerMsg uint64
	// MaxCommittedSizePerReady limits the size of the committed entries which
	// can be applied.
	MaxCommittedSizePerReady uint64
	// MaxUncommittedEntriesSize limits the aggregate byte size of the
	// uncommitted entries that may be appended to a leader's log. Once this
	// limit is exceeded, proposals will begin to return ErrProposalDropped
	// errors. Note: 0 for no limit.
	MaxUncommittedEntriesSize uint64

	CheckQuorum bool
}

// validate raft config validation
func (rc *RaftConfig) validate() error {

	if rc.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if rc.ElectionTick <= rc.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if rc.MaxUncommittedEntriesSize == 0 {
		rc.MaxUncommittedEntriesSize = noLimit
	}

	if rc.MaxCommittedSizePerReady == 0 {
		rc.MaxCommittedSizePerReady = rc.MaxSizePerMsg
	}
	return nil
}

type raft struct {
	id uint64
	// current term
	Term uint64
	// the node you vote for
	Vote uint64
	// lead current leader
	lead uint64
	// raft log information
	raftLog *raftLog
	// the state of current node
	state StateType
	// the message info: compile by protobuf
	//msgs []pb.Message
	// electionElapsed the interval of election
	electionElapsed int
	// heartbeatElapsed heartbeat interval
	heartbeatElapsed int
	pendingConfIndex uint64

	//prs tracker.ProgressTracker
	checkQuorum bool

	maxMsgSize         uint64
	maxUncommittedSize uint64
	electionTimeout    uint64
	heartbeatTimeout   uint64
}

func newRaft(config *RaftConfig) *raft {
	if err := config.validate(); err != nil {
		panic(err.Error())
	}
	// raft log initialization

	//raftlog := newLogWithSize(config.Storage, c.Logger, c.MaxCommittedSizePerReady)
	// state initializetion
	//hs, cs, err := c.Storage.InitialState()
	//if err != nil {
	//	panic(err)
	//}

	r := &raft{
		id:   config.ID,
		lead: None,
		//raftLog:                   raftlog,
		maxMsgSize:         config.MaxSizePerMsg,
		maxUncommittedSize: config.MaxUncommittedEntriesSize,

		//prs:                       tracker.MakeProgressTracker(c.MaxInflightMsgs),
		electionTimeout:  config.ElectionTick,
		heartbeatTimeout: config.HeartbeatTick,
		checkQuorum:      config.CheckQuorum,
	}

	//cfg, prs, err := confchange.Restore(confchange.Changer{
	//	Tracker:   r.prs,
	//	LastIndex: raftlog.lastIndex(),
	//}, cs)
	//if err != nil {
	//	panic(err)
	//}
	//assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))
	//
	//if !IsEmptyHardState(hs) {
	//	r.loadState(hs)
	//}
	//if c.Applied > 0 {
	//	raftlog.appliedTo(c.Applied)
	//}

	//r.becomeFollower(r.Term, None)

	//var nodesStrs []string
	//for _, n := range r.prs.VoterNodes() {
	//	nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	//}

	//log.Printf("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d]",
	//	r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied)
	return r
}
