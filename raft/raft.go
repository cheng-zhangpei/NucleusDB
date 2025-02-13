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

var ErrProposalDropped = errors.New("raft proposal dropped")

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

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
	sendInterval              time.Duration
	CheckQuorum               bool
	Storage                   Storage
	// 状态机内部网络通讯地址
	grpcServerAddr string
	grpcClientAddr string
	// ticker tick最小间隔
	tickInterval time.Duration
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
	raftLog  *raftLog
	readOnly *readOnly
	ms       *MemoryStorage
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
	status         BaseStatus
}

func newRaft(config *RaftConfig) *raft {
	if err := config.validate(); err != nil {
		panic(err.Error())
	}
	// raft log initialization
	raftlog := newLogWithSize(config.Storage, config.MaxCommittedSizePerReady)
	// state initializetion
	//hs, cs, err := config.Storage.InitialState()
	//if err != nil {
	//	panic(err)
	//}
	ms, err := NewMemoryStorage()
	if err != nil {
		return nil
	}
	r := &raft{
		id:                 config.ID,
		lead:               None,
		raftLog:            raftlog,
		ms:                 ms,
		readOnly:           newReadOnly(ReadOnlySafe),
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
	r.electionElapsed++ // so this is not use tick of sys to count heartBeat

	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		var msgType pb.MessageType = pb.MessageType_MsgHup
		if err := r.Step(&pb.Message{From: r.id, Type: msgType}); err != nil {
			log.Println("error occurred during election: %v", err)
		}
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	// ==================election====================
	if r.electionElapsed >= uint64(r.electionTimeout) {
		r.electionElapsed = 0
	}
	//
	if r.state != StateLeader {
		return
	}
	// ==================heartbeat====================
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		var msgType pb.MessageType = pb.MessageType_MsgBeat
		if err := r.Step(&pb.Message{From: r.id, Type: msgType}); err != nil {
			log.Printf("error occurred during checking sending heartbeat: %v\n", err)
		}
	}
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.step = stepFollower
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
	r.step = stepCandidate

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
	r.step = stepLeader
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
func stepLeader(r *raft, msg *pb.Message) error {
	switch msg.Type {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgProp:
		// 处理 MsgProp 类型的消: 确保提案的正确性和合法性，并将日志条目追加到 Raft 日志中
		pr := r.processTracker.Progress[msg.To]

		if len(msg.Entries) == 0 {
			log.Fatalln("%x stepped empty MsgProp", r.id)
		}

		if r.processTracker.Progress[r.id] == nil {
			return ErrProposalDropped
		}
		if !r.appendEntry(convertToPBEntries(msg.Entries)...) {
			return ErrProposalDropped
		}
		// broadcast append msg
		r.bcastAppend()

		switch msg.Type {
		case pb.MessageType_MsgAppResp:
			pr.RecentActive = true
			if msg.Reject {
				log.Println("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d",
					r.id, msg.RejectHint, msg.LogTerm, msg.From, msg.Index)
				nextProbeIdx := msg.RejectHint
				if msg.Term > 0 {
					nextProbeIdx = r.raftLog.findConflictByTerm(msg.RejectHint, msg.LogTerm)
				}
				// update the Next field in progressTrack
				if pr.MaybeDecrTo(msg.Index, nextProbeIdx) {
					log.Println("%x decreased progress of %x to [%s]", r.id, msg.From, pr)
					if pr.State == tracker.StateReplicate {
						pr.BecomeProbe()
					}
					// change to the check model
					r.sendAppend(msg.From)
				}
			} else {
				if pr.MaybeUpdate(msg.Index) {
					switch {
					case pr.State == tracker.StateProbe:
						pr.BecomeReplicate()
						// We've updated flow control information above, which may
						// allow us to send multiple (size-limited) in-flight messages
						// at once (such as when transitioning from probe to
						// replicate, or when freeTo() covers multiple messages). If
						// we have more entries to send, send as many messages as we
						// can (without sending empty messages for the commit index)
						for r.maybeSendAppend(msg.From, false) {

						}
					}
				}
			}
		case pb.MessageType_MsgHeartbeatResp:
			pr.RecentActive = true
			// 在心跳中 不处于冲突检测阶段
			pr.ProbeSent = false
			// free one slot for the full inflights window to allow progress.
			if pr.State == tracker.StateReplicate && pr.UncertainMessage.Full() {
				// 现在环形缓冲区已经满了
				pr.UncertainMessage.FreeFirstOne()
			}
			// 对方匹配的数据比现在的最后的一条日志数据还要少，所以在心跳的时候继续将缓冲区的数据发出去
			if pr.Match < r.raftLog.lastIndex() {
				r.sendAppend(msg.From)
			}
		}
	}
	return nil
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}
func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.processTracker.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id, ctx)
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(r.processTracker.Progress[to].Match, r.raftLog.committed)
	var msgType pb.MessageType = pb.MessageType_MsgHeartbeat
	m := &pb.Message{
		To:      to,
		Type:    msgType,
		Commit:  commit,
		Context: ctx,
	}
	r.send(m)
}

// stepCandidate send message to candidate
func stepCandidate(r *raft, msg *pb.Message) error {
	switch msg.Type {
	case pb.MessageType_MsgProp:
		log.Fatalln("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgApp:
		r.becomeFollower(msg.Term, msg.From) // always m.Term == r.Term
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(msg.Term, msg.From) // always m.Term == r.Term
		r.handleHeartbeat(msg)
	case pb.MessageType_MsgAppResp:
		r.becomeFollower(r.Term, None)
		return nil
	case pb.MessageType_MsgVoteResp:
		r.handleVoteResponse(msg)
		return nil
	}
	return nil
}

// stepFollower send message to follower
func stepFollower(r *raft, msg *pb.Message) error {
	switch msg.Type {
	case pb.MessageType_MsgProp:
		if r.lead == None {
			log.Println("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		}
		msg.To = r.lead
		r.send(msg)
	case pb.MessageType_MsgApp:
		r.electionElapsed = 0
		r.lead = msg.From
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = msg.From
		r.handleHeartbeat(msg)
	}
	return nil
}

// ==================接受到下面两种信息需要如何修改自身状态===================
func (r *raft) handleAppendEntries(msg *pb.Message) {
	if msg.Index < r.raftLog.committed {
		// follower committed的数据新
		r.send(&pb.Message{To: msg.From, Type: pb.MessageType_MsgAppResp, Index: r.raftLog.committed})
		return
	}
	// 这些数据可以放入ms的缓冲区中
	if _, ok := r.raftLog.AppendWithConflictCheck(msg); ok {
		r.send(&pb.Message{To: msg.From, Type: pb.MessageType_MsgAppResp, Index: r.ms.lastIndex()})
	} else {
		// 到这个位置说明第一个位置都没法匹配咯,要立刻开启探针模式
		hintIndex := min(msg.Index, r.raftLog.lastIndex())
		hintIndex = r.raftLog.findConflictByTerm(hintIndex, msg.LogTerm)
		hintTerm, err := r.raftLog.term(hintIndex)
		if err != nil {
			panic(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
		}
		r.send(&pb.Message{
			To:         msg.From,
			Type:       pb.MessageType_MsgAppResp,
			Index:      msg.Index,
			Reject:     true,
			RejectHint: hintIndex,
			LogTerm:    hintTerm,
		})
	}
}
func (r *raft) handleHeartbeat(m *pb.Message) {
	// 确保与 Leader 的Committed(Leader 已提交的最高日志索引)状态一致。
	r.raftLog.commitTo(m.Commit)
	r.send(&pb.Message{To: m.From, Type: pb.MessageType_MsgHeartbeatResp, Context: m.Context})
}

// Step the entry of all the message
func (r *raft) Step(msg *pb.Message) error {
	// Check if the message has a higher term than the current Raft node's term
	// 如果消息的任期大于当前 Raft 节点的任期
	if msg.Term > r.Term {
		// 根据消息类型处理不同情况
		switch msg.Type {
		// 如果是投票请求消息，将当前节点转变为跟随者，领导者 ID 设置为 None
		case pb.MessageType_MsgVote:
			r.becomeFollower(msg.Term, None)
		// 对于其他消息类型，将当前节点转变为跟随者，并更新领导者 ID 为消息发送者 ID
		default:
			r.becomeFollower(msg.Term, msg.From)
		}
	}
	// 根据消息类型处理不同消息
	switch msg.Type {
	// MsgHup：触发选举流程
	case pb.MessageType_MsgHup:
		r.startElection(msg)
	// MsgVote：处理投票请求
	case pb.MessageType_MsgVote:
		r.handleVoteRequest(msg)
	default:
		// down to the detail disposal of different status
		err := r.step(r, msg)
		if err != nil {
			return err
		}
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
}

// handleVoteRequest处理投票请求消息，决定是否授予投票。
func (r *raft) handleVoteRequest(msg *pb.Message) {
	if r.state == StateLeader {
		log.Println("leader can not handle the vote request")
		return
	}
	// two factor: 1. the node receive the msg that it has voted. 2. do not vote yet
	canVote := r.Vote == msg.From ||
		(r.Vote == None && r.lead == None)
	//if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) { // judge if the candidate have the qualification to ask for vote
	//
	//}
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	var msgType pb.MessageType = pb.MessageType_MsgVoteResp
	if canVote {
		log.Println("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, msg.Type, msg.From, msg.Term, msg.Index, r.Term)

		r.send(&pb.Message{To: msg.From, Term: msg.Term, Type: msgType})
		r.electionElapsed = 0
		r.Vote = msg.From
	} else {
		log.Println("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, msg.Type, msg.From, msg.Term, msg.Index, r.Term)
		var reject = true
		r.send(&pb.Message{To: msg.From, Term: r.Term, Type: msgType, Reject: reject})
	}

}

// handleVoteResponse 处理投票响应消息，更新投票状态并检查是否当选。execute by leader
func (r *raft) handleVoteResponse(msg *pb.Message) {
	// update the value
	r.votes[msg.From] = true

	if r.poll(r.id, true) == true {
		r.becomeLeader()
		return
	}
}

// send put the msg into the msg pending area.send msg then node is ready
// 在Raft 的主循环中，节点会定期调用 Ready 方法来检查是否有需要处理的工作。
func (r *raft) send(msg *pb.Message) {
	if msg.From == None {
		msg.From = r.id
	}
	if msg.Type == pb.MessageType_MsgVote || msg.Type == pb.MessageType_MsgVoteResp {
		if msg.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", msg.Type))
		}
	} else {
		if msg.Term != 0 {
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
	var lastIndex uint64 = r.raftLog.lastIndex()
	msg := &pb.Message{
		Type:  msgType,   // *MessageType
		Term:  r.Term,    // *uint64
		To:    id,        // *uint64
		From:  r.id,      // *uint64
		Index: lastIndex, // *uint64

	}
	r.send(msg)
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (r *raft) bcastAppend() {
	r.processTracker.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		r.sendAppend(id)
	})
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer.
func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.processTracker.Progress[to]
	if pr.IsPaused() {
		return false
	}
	m := pb.Message{}
	m.To = to

	term, _ := r.raftLog.term(pr.Next - 1)
	// 将follower的想要的数据之后的数据段发送出去，这里是直接发送到结尾
	ents := r.ms.ents[pr.Next:]
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}
	var entries []*pb.Entry
	for _, entry := range ents {
		entries = append(entries, entry)
	}
	// 封装消息数据
	m.Type = pb.MessageType_MsgApp
	m.Index = pr.Next - 1
	m.Term = term
	m.Entries = entries
	m.Commit = r.raftLog.committed
	if n := len(m.Entries); n != 0 {
		switch pr.State {
		// optimistically increase the next when in StateReplicate
		case tracker.StateReplicate:
			last := m.Entries[n-1].Index
			pr.OptimisticUpdate(last)
			pr.UncertainMessage.Add(last)
		case tracker.StateProbe:
			pr.ProbeSent = true
		default:
			log.Fatalln("%x is sending append in unhandled state %s", r.id, pr.State)
		}
	}
	r.send(&m)

	return true
}

//todo 与上层数据的交互暂时先不说

//func (r *raft) advance(rd Ready) {
//	r.reduceUncommittedSize(rd.CommittedEntries)
//	// If entries were applied (or a snapshot), update our cursor for
//	// the next Ready. Note that if the current HardState contains a
//	// new Commit index, this does not mean that we're also applying
//	// all of the new entries due to commit pagination by size.
//	if newApplied := rd.appliedCursor(); newApplied > 0 {
//		oldApplied := r.raftLog.applied
//		r.raftLog.appliedTo(newApplied)
//
//		if r.prs.Config.AutoLeave && oldApplied <= r.pendingConfIndex && newApplied >= r.pendingConfIndex && r.state == StateLeader {
//			// If the current (and most recent, at least for this leader's term)
//			// configuration should be auto-left, initiate that now. We use a
//			// nil Data which unmarshals into an empty ConfChangeV2 and has the
//			// benefit that appendEntry can never refuse it based on its size
//			// (which registers as zero).
//			ent := pb.Entry{
//				Type: pb.EntryConfChangeV2,
//				Data: nil,
//			}
//			// There's no way in which this proposal should be able to be rejected.
//			if !r.appendEntry(ent) {
//				panic("refused un-refusable auto-leaving ConfChangeV2")
//			}
//			r.pendingConfIndex = r.raftLog.lastIndex()
//			r.logger.Infof("initiating automatic transition out of joint configuration %s", r.prs.Config)
//		}
//	}
//
//	if len(rd.Entries) > 0 {
//		e := rd.Entries[len(rd.Entries)-1]
//		r.raftLog.stableTo(e.Index, e.Term)
//	}
//	if !IsEmptySnap(rd.Snapshot) {
//		r.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
//	}
//}

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
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= uint64(r.randomizedElectionTimeout)
}

//	func (r *raft) advance(rd *Ready) {
//		// 处理已提交的日志条目
//		r.reduceUncommittedSize(rd.CommittedEntries)
//	}
func (l *raftLog) term(i uint64) (uint64, error) {
	// the valid term range is [index of dummy entry, last index]
	dummyIndex := l.firstIndex() - 1
	// out of bounder
	if i < dummyIndex || i > l.lastIndex() {
		// TODO: return an error instead?
		return 0, nil
	}
	t, err := l.ms.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}
func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() *HardState {
	return &HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

// the uncommitted entry size limit.
func (r *raft) reduceUncommittedSize(ents []*pb.Entry) {
	if r.uncommittedSize == 0 {
		// Fast-path for followers, who do not track or enforce the limit.
		return
	}

	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}
func convertToPBEntries(entries []*pb.Entry) []pb.Entry {
	pbEntries := make([]pb.Entry, len(entries))
	for i, entry := range entries {
		pbEntries[i] = *entry // 假设 Entry 和 pb.Entry 是兼容的
	}
	return pbEntries
}
