package raft

import (
	"ComDB/raft/pb"
	"ComDB/raft/tracker"
	_ "ComDB/raft/tracker"
	_ "bytes"
	"errors"
	"fmt"
	"golang.org/x/exp/rand"
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

const None uint64 = 0
const noLimit = math.MaxUint64

type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
}

type stepFunc func(r *raft, msg *pb.Message) error

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
	msgs []*pb.Message
	// record if the peer vote
	votes []bool
	// electionElapsed the interval of election
	electionElapsed uint64
	// heartbeatElapsed heartbeat interval
	heartbeatElapsed uint64
	pendingConfIndex uint64
	uncommittedSize  uint64
	//prs tracker.ProgressTracker
	checkQuorum               bool
	randomizedElectionTimeout int
	maxMsgSize                uint64
	maxUncommittedSize        uint64
	electionTimeout           int
	heartbeatTimeout          uint64
	// to trigger different function like heartbeat or election
	tick           func()
	step           stepFunc
	processTracker tracker.ProgressTracker
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
		electionTimeout:  int(config.ElectionTick),
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

// reset Reset the state of the raft instance
func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// generate randomized Election timeout
	r.resetRandomizedElectionTimeout()

	r.pendingConfIndex = 0
	r.uncommittedSize = 0
}

// appendEntry todo: after the storage module finished
func (r *raft) appendEntry(es ...pb.Entry) (accepted bool) {
	return true
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {
	r.electionElapsed++

	//if r.promotable() && r.pastElectionTimeout() {
	//	r.electionElapsed = 0
	//	if err := r.Step(pb.Message{From: r.id, Type: pb.MsgHup}); err != nil {
	//		r.logger.Debugf("error occurred during election: %v", err)
	//	}
	//}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	// ==================election====================
	if r.electionElapsed >= uint64(r.electionTimeout) {
		r.electionElapsed = 0
		if r.checkQuorum {
			// todo: step....
			//if err := r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum}); err != nil {
			//	r.logger.Debugf("error occurred during checking sending heartbeat: %v", err)
			//}
		}
	}
	//
	if r.state != StateLeader {
		return
	}
	// ==================heartbeat====================
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		// todo: step....
		//if err := r.Step(pb.Message{From: r.id, Type: pb.MsgBeat}); err != nil {
		//	r.logger.Debugf("error occurred during checking sending heartbeat: %v", err)
		//}
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	log.Println("%x became follower at term %d", r.id, r.Term)
}

func (r *raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.state = StateCandidate
	log.Println("%x became candidate at term %d", r.id, r.Term)
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	//r.pendingConfIndex = r.raftLog.lastIndex()

	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		// This won't happen because we just called reset() above.
		log.Println("empty entry was dropped")
	}
	//r.reduceUncommittedSize([]pb.Entry{emptyEnt})
	log.Println("%x became leader at term %d", r.id, r.Term)
}

// ===========================================================state change==========================================
// stepLeader send message to leader
func stepLeader(r *raft, msg *pb.Message) error { return nil }

// stepCandidate send message to candidate
func stepCandidate(r *raft, msg *pb.Message) error { return nil }

// stepFollower send message to follower
func stepFollower(r *raft, msg *pb.Message) error { return nil }

// Step the entry of all the message
func (r *raft) Step(msg *pb.Message) error {
	// Check if the message has a higher term than the current Raft node's term
	// 如果消息的任期大于当前 Raft 节点的任期
	if *msg.Term > r.Term {
		// 根据消息类型处理不同情况
		switch *msg.Type {
		// 如果是投票请求消息，将当前节点转变为跟随者，领导者 ID 设置为 None
		case pb.MessageType_MsgVote:
			r.becomeFollower(*msg.Term, None)
		// 对于其他消息类型，将当前节点转变为跟随者，并更新领导者 ID 为消息发送者 ID
		default:
			r.becomeFollower(*msg.Term, *msg.From)
		}
	}

	// 根据消息类型处理不同消息
	switch *msg.Type {
	// MsgHup：触发选举流程
	case pb.MessageType_MsgHup:
		r.startElection(msg)

	// MsgVote：处理投票请求
	case pb.MessageType_MsgVote:
		r.handleVoteRequest(msg)

	// MsgVoteResp：处理投票响应
	case pb.MessageType_MsgVoteResp:
		r.handleVoteResponse(msg)

	// MsgAppend：处理日志追加请求
	case pb.MessageType_MsgApp:
		r.handleAppendEntries(msg)

	// MsgAppendResp：处理日志追加响应
	case pb.MessageType_MsgAppResp:
		r.handleAppendEntriesResponse(msg)
	}

	// 返回 nil 表示处理成功
	return nil
}

// =======================================================================handle different message ==============================================
// startElection触发选举流程，使当前节点转变为候选者并开始新的选举。
// only candidate
func (r *raft) startElection(msg *pb.Message) {
	if r.state == StateLeader {
		log.Println("the node already is leader")
		return
	}
	r.becomeCandidate()
	// vote self
	r.votes[r.id] = true
	// send vote request
	r.sendVoteRequests()
	// judge if the candidate can be the leader
	if r.poll(r.id, true) == true {
		r.becomeLeader()
		return
	}
}

// handleVoteRequest处理投票请求消息，决定是否授予投票。
func (r *raft) handleVoteRequest(msg *pb.Message) {

}

// handleVoteResponse处理投票响应消息，更新投票状态并检查是否当选。
func (r *raft) handleVoteResponse(msg *pb.Message) {

}

// handleAppendEntries处理日志追加请求，更新日志并发送响应。
func (r *raft) handleAppendEntries(msg *pb.Message) {

}

// handleAppendEntriesResponse处理日志追加响应消息，更新领导者进度。
func (r *raft) handleAppendEntriesResponse(msg *pb.Message) {

}

// send put the msg into the msg pending area.send msg then node is ready
// 在Raft 的主循环中，节点会定期调用 Ready 方法来检查是否有需要处理的工作。
func (r *raft) send(msg *pb.Message) {
	if *msg.From == None {
		*msg.From = r.id
	}
	if *msg.Type == pb.MessageType_MsgVote || *msg.Type == pb.MessageType_MsgVoteResp {
		if *msg.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", msg.Type))
		}
	} else {
		if *msg.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", msg.Type, msg.Term))
		}
	}
	r.msgs = append(r.msgs, msg)
}
func (r *raft) poll(id uint64, voteGranted bool) bool {
	// 记录投票
	r.votes[id] = voteGranted

	// 更新投票状态
	granted, rejected := 0, 0
	for _, voted := range r.votes {
		if voted {
			granted++
		} else {
			rejected++
		}
	}

	// 判断是否赢得多数票
	if granted > len(r.votes)/2 {
		log.Println("%x won the election with %d votes", r.id, granted)
		return true
	} else if rejected > len(r.votes)/2 {
		log.Println("%x lost the election with %d votes", r.id, rejected)
		return false
	}
	return false
}
func (r *raft) sendVoteRequests() {
	var ids []uint64
	{
		// get all raftNode ids
		idMap := r.processTracker.Votes
		ids = make([]uint64, 0, len(idMap))
		for id := range idMap {
			ids = append(ids, id)
		}
		// sort id array
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	for _, id := range ids {
		if id == r.id {
			continue
		}
		r.sendVoteRequest(id)
	}
}

func (r *raft) sendVoteRequest(id uint64) {
	// 创建一个变量来存储 MessageType_MsgVote，并取其地址
	var msgType pb.MessageType = pb.MessageType_MsgVote

	msg := &pb.Message{
		Type: &msgType, // *MessageType
		Term: &r.Term,  // *uint64
		To:   &id,      // *uint64
		From: &r.id,    // *uint64
		//todo after storage finished
		//Index:   &r.raftLog.lastIndex(),     // *uint64
		//LogTerm: &r.raftLog.lastTerm(),      // *uint64
	}
	r.send(msg)
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// Intn we should ensure the generation of random election timeout atomic
func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}
