package raft

import (
	"ComDB"
	"ComDB/raft/pb"
	"ComDB/raft/tracker"
	_ "ComDB/raft/tracker"
	"ComDB/raft/txn"
	_ "bytes"
	"errors"
	"fmt"
	"golang.org/x/exp/rand"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
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
	ID                        uint64        `yaml:"id" env:"NODE_ID"`
	ElectionTick              uint64        `yaml:"election_tick" env:"ELECTION_TICK"`
	HeartbeatTick             uint64        `yaml:"heartbeat_tick" env:"HEARTBEAT_TICK"`
	MaxSizePerMsg             uint64        `yaml:"max_size_per_msg" env:"MAX_SIZE_PER_MSG"`
	MaxCommittedSizePerReady  uint64        `yaml:"max_committed_size_per_ready" env:"MAX_COMMITTED_SIZE_PER_READY"`
	MaxUncommittedEntriesSize uint64        `yaml:"max_uncommitted_entries_size" env:"MAX_UNCOMMITTED_ENTRIES_SIZE"`
	SendInterval              time.Duration `yaml:"send_interval" env:"SEND_INTERVAL"`
	CheckQuorum               bool          `yaml:"check_quorum" env:"CHECK_QUORUM"`
	GRPCServerAddr            string        `yaml:"grpc_server_addr" env:"GRPC_SERVER_ADDR"`
	GRPCClientAddr            []string      `yaml:"grpc_client_addr" env:"GRPC_CLIENT_ADDRS" envSeparator:","`
	TickInterval              time.Duration `yaml:"tick_interval" env:"TICK_INTERVAL"`
	HttpServerAddr            string        `yaml:"http_server_addr" env:"HTTP_SERVER_ADDR"`
	HttpServerAddrs           []string      `yaml:"http_server_addrs" env:"HTTP_SERVER_ADDRS"`
	InflghtsMaxSize           int           `yaml:"inflghts_max_size" env:"INFLIGHTS_MAX_SIZE"`
	zookeeprAddr              string        `yaml:"zookeeper_addr" env:"ZKADDR"`
	CoordinatorServerAddr     string        `yaml:"coordinator_server_addr" env:"COORDINATORSERVERADDR"`
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
	// 用于标识candidate是否已经发送了投票数据
	voted bool
	// 用于标识candidate重新投票的间隔，只有达到了这个间隔才会发生重新投票，暂定为ElectionTimeout的十倍
	votedExpire uint64
	// lead current leader
	lead     uint64
	isLeader bool
	// raft log information

	raftLog  *raftLog
	readOnly *readOnly
	ms       *MemoryStorage
	// the state of current node
	state StateType
	// the message info: compile by protobuf
	msgs []*pb.Message
	// electionElapsed the interval of election
	electionElapsed uint64
	// heartbeatElapsed heartbeat interval
	heartbeatElapsed uint64
	pendingConfIndex uint64
	uncommittedSize  uint64
	//prs tracker.ProgressTracker
	checkQuorum        bool
	maxMsgSize         uint64
	maxUncommittedSize uint64
	electionTimeout    int
	heartbeatTimeout   uint64
	// to trigger different function like heartbeat or election
	tick           func()
	step           stepFunc
	processTracker *tracker.ProgressTracker
	status         BaseStatus
	app            *application
	coordinator    *txn.Coordinator
	leaseManager   *txn.TimestampLeaseManager
	// 事务上下界时间戳
	logicalTs uint64
	// 事务数据暂存区
	txnSnapshot *txn.TxnSnapshot
	txnBuffer   []*pb.TxnOperation
	// 协调者客户端
	coordinatorClient *txn.CoordinatorClient
	// todo 这个有可能和其他事务串了，这个设计不大好，一个异步的事务内容缓冲区后续设计最好是要结合事务号落盘比较好

	txnContent [][]byte
}

func newRaft(config *RaftConfig, options ComDB.Options) *raft {
	if err := config.validate(); err != nil {
		panic(err)
	}
	// raft log initialization
	ms, _ := NewMemoryStorage()

	raftlog := newLogWithSize(ms, config.MaxCommittedSizePerReady)
	// state initializetion
	//hs, cs, err := config.Storage.InitialState()
	//if err != nil {
	//	panic(err)
	//}
	config.zookeeprAddr = "127.0.0.1:2181"
	coordinator := txn.NewCoordinator(config.zookeeprAddr)
	electionTimeout := configElectionTimeout(config.TickInterval)
	heartbeatTimeout := configHeartbeatTimeout(time.Duration(electionTimeout), config.TickInterval)
	appl, err := newApplication(options, config.ID)
	zkConn := txn.NewZookeeperConn([]string{config.zookeeprAddr}, 5*time.Second, nil)
	leaseManager := txn.NewTimestampLeaseManager(zkConn, "")
	txnBuffer := make([]*pb.TxnOperation, 0)
	coorClient := txn.NewCoordinatorClient(config.CoordinatorServerAddr)
	txnSnapshot := txn.NewTxnSnapshot()
	txnContent := make([][]byte, 0)
	if err != nil {
		panic(err)
	}

	r := &raft{
		id: config.ID,

		lead:               0,
		raftLog:            raftlog,
		ms:                 ms,
		readOnly:           newReadOnly(ReadOnlySafe),
		isLeader:           false,
		maxMsgSize:         config.MaxSizePerMsg,
		maxUncommittedSize: config.MaxUncommittedEntriesSize,
		voted:              false,
		votedExpire:        uint64(electionTimeout * 10),
		msgs:               make([]*pb.Message, 0),
		electionTimeout:    electionTimeout,
		heartbeatTimeout:   uint64(heartbeatTimeout),
		checkQuorum:        config.CheckQuorum,
		app:                appl,
		coordinator:        coordinator,
		leaseManager:       leaseManager,
		txnBuffer:          txnBuffer,
		txnSnapshot:        txnSnapshot,
		coordinatorClient:  coorClient,
		txnContent:         txnContent,
	}
	r.processTracker = tracker.MakeProgressTracker(config.InflghtsMaxSize)
	// 需要对processTracker进行初始化
	for i := 1; i <= len(config.GRPCClientAddr); i++ {
		r.processTracker.Votes[uint64(i)] = false
		r.processTracker.TxnCommitted[uint64(i)] = false
	}
	// 初始化progressMap，如果不初始化无法进行心跳,因为每一个node都有可能变为Leader所以在初始化的时候都要进行创建
	// tou ge lan hhhh
	for i := 1; i <= len(config.GRPCClientAddr); i++ {
		// 新建每个节点的Progress
		progress := tracker.NewProgress(config.InflghtsMaxSize)
		r.processTracker.Progress[uint64(i)] = progress
	}

	//todo 这个清除租约的位置是否可以放到别的位置？
	err = r.leaseManager.ReleaseLease()
	if err != nil {
		log.Println(err)
	}
	r.becomeFollower(r.Term, None)
	return r
}
func configElectionTimeout(interval time.Duration) int {
	var time_up time.Duration = 300
	var time_lo time.Duration = 150
	up_bo := int(time_up / interval)
	lo_bo := int(time_lo / interval)
	// 生成一个介于 lo_bo 和 uep_bo 之间的随机数
	randomNum := rand.Intn(up_bo-lo_bo+1) + lo_bo
	//log.Printf("generate electionTimeout %d, interval %d\n", randomNum, interval)
	return randomNum
}

// configHeartbeatTimeout 根据选举超时计算心跳超时
func configHeartbeatTimeout(electionTimeout time.Duration, interval time.Duration) (heartbeatTimeout time.Duration) {
	// 设置心跳超时的最小值（例如 3 个 Tick）
	minHeartbeatTimeout := 3

	// 心跳超时通常是选举超时的 1/10，但不能小于最小值
	heartbeatTimeout = electionTimeout / 10
	if heartbeatTimeout < time.Duration(minHeartbeatTimeout) {
		heartbeatTimeout = time.Duration(minHeartbeatTimeout)
	}
	return heartbeatTimeout
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

func (r *raft) appendEntry(es []*pb.Entry) (accepted bool) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + uint64(i)
	}
	// 确保未提交的数据更新到uncommitedSize中，这里是提交的字节数量
	if !r.increaseUncommittedSize(es) {
		log.Printf(
			"%x appending new entries to log would exceed uncommitted entry size limit; dropping proposal\n",
			r.id,
		)
		// Drop the proposal.
		return false
	}
	// use latest "last" index after truncate/append
	lastIndex := r.raftLog.storage.Append(es)
	// 更新自己的progress状态说明自己最新的匹配位置并且还要说明现在当前节点不属于探针状态
	r.processTracker.Progress[r.id].MaybeUpdate(uint64(lastIndex))
	//// Regardless of maybeCommit's return, our caller will call bcastAppend.
	// 这里先更新commited的参数实际这个地方还没有更新，因为这里设计的数据更新是异步的
	r.maybeCommit()
	return true
}

// maybeCommit 检查当前 Raft 节点的日志条目是否可以被提交（committed）。在 Raft 协议中，日志条目只有在被大多数
// 节点复制后才能被认为是已提交的。r.maybeCommit() 会检查每个节点的 Match 指针，确定是否有足够的节点
// 已经复制了某个日志条目，从而推进 CommitIndex
func (r *raft) maybeCommit() bool {
	//遍历所有节点的 Match 指针，收集每个节点的最新匹配日志索引。
	//使用这些索引计算当前可以提交的日志索引。
	//更新 CommitIndex，使其指向可以提交的日志条目
	commonMatchIndex := r.processTracker.Committed()
	// 判断是否需要更新Committed指针
	return r.raftLog.maybeCommit(commonMatchIndex, r.Term)
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {
	r.electionElapsed++ // so this is not use tick of sys to count heartBeat

	if r.pastElectionTimeout() {
		var msgType pb.MessageType = pb.MessageType_MsgHup
		if err := r.Step(&pb.Message{From: r.id, Type: msgType}); err != nil {
			log.Printf("error occurred during election: %v\n", err)
		}
	}
}
func (r *raft) increaseUncommittedSize(ents []*pb.Entry) bool {
	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}

	if r.uncommittedSize > 0 && s > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		// If the uncommitted tail of the Raft log is empty, allow any size
		// proposal. Otherwise, limit the size of the uncommitted tail of the
		// log and drop any proposal that would push the size over the limit.
		// Note the added requirement s>0 which is used to make sure that
		// appending single empty entries to the log always succeeds, used both
		// for replicating a new leader's initial empty entry, and for
		// auto-leaving joint configurations.
		return false
	}
	r.uncommittedSize += s
	return true
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

// todo 此处释放租约的错误处理
func (r *raft) becomeFollower(term uint64, lead uint64) error {
	if r.state == StateLeader {
		err := r.leaseManager.ReleaseLease()
		if err != nil {
			return err
		}
	}
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.step = stepFollower
	r.isLeader = false
	log.Printf("%x became follower at term %d\n", r.id, r.Term)
	return nil
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

	log.Printf("%x became candidate at term %d\n", r.id, r.Term)
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.leaseManager.LeaderId = strconv.FormatUint(r.id, 10)

	logicalTs, err := r.leaseManager.AcquireLease()
	log.Printf("raft node[%d] acquired lease at term %d and logical timestamp:%d", r.id, r.Term, r.logicalTs)
	if err != nil {
		panic(err)
	}
	r.logicalTs = logicalTs
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	r.isLeader = true

	// 这里不要往里面去放一个空白的日志，我的index是从0开始的
	//r.reduceUncommittedSize([]pb.Entry{emptyEnt})
	log.Printf("%x became leader at term %d\n", r.id, r.Term)
}

// ===========================================================state change==========================================
// stepLeader send message to leader
func stepLeader(r *raft, msg *pb.Message) error {
	pr := r.processTracker.Progress[msg.To]
	switch msg.Type {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgProp:
		// 处理 MsgProp 类型的消: 确保提案的正确性和合法性，并将日志条目追加到 Raft 日志中
		if len(msg.Entries) == 0 {
			log.Fatalf("%x stepped empty MsgProp\n", r.id)
		}
		if r.processTracker.Progress[r.id] == nil {
			return ErrProposalDropped
		}
		if !r.appendEntry(msg.Entries) {
			return ErrProposalDropped
		}
		// broadcast append msg
		r.bcastAppend()
	case pb.MessageType_MsgAppResp:
		pr.RecentActive = true
		if msg.Reject {
			log.Printf("%x received MsgAppResp(rejected, hint: (index %d, term %d)) from %x for index %d\n",
				r.id, msg.RejectHint, msg.LogTerm, msg.From, msg.Index)
			nextProbeIdx := msg.RejectHint
			if msg.Term > 0 {
				nextProbeIdx = r.raftLog.findConflictByTerm(msg.RejectHint, msg.LogTerm)
			}
			// update the Next field in progressTrack
			if pr.MaybeDecrTo(msg.Index, nextProbeIdx) {
				log.Printf("%x decreased progress of %x to [%s]\n", r.id, msg.From, pr)
				if pr.State == tracker.StateReplicate {
					pr.BecomeProbe()
				}
				// change to the check model
				r.sendAppend(msg.From)
			}
		} else {
			// 应用日志
			r.app.commitc <- msg.Entries
			// 应用到状态机上
			r.app.applyc <- getCommand(msg.Entries)
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
	case pb.MessageType_MsgCommitTxn:
		// 这个时候Leader收到CommitTxn请求而启动日志处理，这里是独立于基础操作之外的一个另一个操作所以需要重新定义执行逻辑
		pr.RecentActive = true
		// 封装message
		msg := r.TxnSnapshotToMsg()
		r.sendTxnRequest(msg)
	case pb.MessageType_MsgCommitTxnResp:
		pr.RecentActive = true
		// 对其他节点事务的回复进行处理
		err := r.handleTxnCommitResp(msg)
		if err != nil {
			return err
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
		log.Fatalf("%x no leader at term %d; dropping proposal\n", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgApp:
		_ = r.becomeFollower(msg.Term, msg.From) // always m.Term == r.Term
		r.handleAppendEntries(msg)
		println()
	case pb.MessageType_MsgHeartbeat:
		_ = r.becomeFollower(msg.Term, msg.From) // always m.Term == r.Term
		r.handleHeartbeat(msg)
	case pb.MessageType_MsgAppResp:
		_ = r.becomeFollower(r.Term, None)
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
			log.Printf("%x no leader at term %d; dropping proposal\n", r.id, r.Term)
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
	case pb.MessageType_MsgCommitTxn:
		r.handleTxnRequest(msg)
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
		// 将日志给提交
		r.app.commitc <- msg.Entries
		// 应用到状态机上
		r.app.applyc <- getCommand(msg.Entries)
		// 将已经提交的数据再次发送给leader，让leader也进行提交
		r.send(&pb.Message{To: msg.From, Type: pb.MessageType_MsgAppResp, Index: r.ms.lastIndex(), Entries: msg.Entries})
	} else {
		// 到这个位置说明第一个位置都没法匹配咯,要立刻开启探针模式 todo 这个后面的内容暂时可能没法测，工作量太大了

		hintIndex := min(msg.Index, r.raftLog.lastIndex())
		hintIndex = r.raftLog.findConflictByTerm(hintIndex, msg.LogTerm)
		hintTerm, err := r.raftLog.storage.Term(hintIndex)
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
			log.Printf("Raft Node %d get vote request from node %d", r.id, msg.From)
			_ = r.becomeFollower(msg.Term, None)
		// 对于其他消息类型，将当前节点转变为跟随者，并更新领导者 ID 为消息发送者 ID
		default:
			_ = r.becomeFollower(msg.Term, msg.From)
		}
	}
	// 根据消息类型处理不同消息
	switch msg.Type {
	// MsgHup：触发选举流程
	case pb.MessageType_MsgHup:
		// 在这里会不断触发选举，所以在start之后一次就要停住了
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

// ==================================handle different message =============================
// startElection触发选举流程，使当前节点转变为候选者并开始新的选举。
// only candidate
func (r *raft) startElection(msg *pb.Message) {
	log.Printf("node[%d]start election!\n", r.id)
	if r.state == StateLeader {
		log.Printf("the node already is leader")
		return
	}
	// 这些条件的限制是为了避免节点反复的发生状态的转化而导致资源浪费
	if (r.voted == false) || (r.voted == true && r.electionElapsed == r.votedExpire) {
		r.becomeCandidate()
		r.processTracker.Votes[r.id] = true
		r.electionElapsed = 0
		r.sendVoteRequests()
	}
	// judge if the candidate can be the leader
}

// handleVoteRequest处理投票请求消息，决定是否授予投票。
func (r *raft) handleVoteRequest(msg *pb.Message) {
	if r.state == StateLeader {
		log.Printf("leader can not handle the vote request")
		return
	}
	// 如果拿到了投票的请求就别再继续选拔了，但是也不用转成follower，candidate也可以去投票
	r.electionElapsed = 0
	// two factor: 1. the node receive the msg that it has voted. 2. do not vote yet
	// 真的忍不住想要吐槽，vote和leader被初始化为0了，我就说为啥会疯狂被拒绝.....
	canVote := r.Vote == 0 && r.lead == 0
	//if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) { // judge if the candidate have the qualification to ask for vote
	//
	//}
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	var msgType pb.MessageType = pb.MessageType_MsgVoteResp
	if canVote {
		log.Printf("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d\n",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, msg.Type, msg.From, msg.Term, msg.Index, r.Term)

		r.send(&pb.Message{To: msg.From, Term: msg.Term, Type: msgType})
		r.electionElapsed = 0
		r.Vote = msg.From
	} else {
		log.Printf("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d\n",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, msg.Type, msg.From, msg.Term, msg.Index, r.Term)
		var reject = true
		r.send(&pb.Message{To: msg.From, Term: r.Term, Type: msgType, Reject: reject})
	}
}

// handleVoteResponse 处理投票响应消息，更新投票状态并检查是否当选。execute by leader
func (r *raft) handleVoteResponse(msg *pb.Message) {
	// update the value
	r.processTracker.Votes[msg.From] = true
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
	if msg.Type == pb.MessageType_MsgVote || msg.Type == pb.MessageType_MsgVoteResp || msg.Type == pb.MessageType_MsgCommitTxn {
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
	r.processTracker.Votes[id] = voteGranted
	// 更新投票状态
	granted, rejected := 0, 0
	for _, voted := range r.processTracker.Votes {
		if voted {
			granted++
		} else {
			rejected++
		}
	}
	// 判断是否赢得多数票
	if granted > len(r.processTracker.Votes)/2 {
		log.Printf("%x won the election with %d votes\n", r.id, granted)
		return true
	} else if rejected > len(r.processTracker.Votes)/2 {
		log.Printf("%x lost the election with %d votes\n", r.id, rejected)
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
func (r *raft) bcastTxnMessage(TxnContextId uint64, TxnPackage *pb.TransactionPackage, Phase pb.TransactionPhase) {
	r.processTracker.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id {
			return
		}
		var msgType pb.MessageType = pb.MessageType_MsgCommitTxn
		// 构造事务CommitRequest
		msg := &pb.Message{
			Type:         msgType,
			From:         r.id,
			To:           id,
			Term:         r.Term,
			TxnPhase:     Phase,
			TxnContextId: TxnContextId,
			TxnPackage:   TxnPackage,
		}
		r.send(msg)
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

	term, _ := r.raftLog.storage.Term(pr.Next - 1)
	// 将follower的想要的数据之后的数据段发送出去，这里是直接发送到结尾
	ents := r.ms.ents[pr.Next-1:]
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
	// 接受者的对应接受位置leader的任期
	m.LogTerm = term
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
			log.Fatalf("%x is sending append in unhandled state \n", r.id)
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
	// 使用当前时间作为随机数生成器的种子
	rand.Seed(uint64(time.Now().UnixNano()))

	// 定义随机偏移量的范围为选举超时时间的 50%
	maxOffset := r.electionTimeout / 2

	// 确保 maxOffset 是正数
	if maxOffset <= 0 {
		maxOffset = 1
	}

	// 生成随机偏移量
	randomOffset := time.Duration(rand.Int63n(int64(maxOffset)))

	// 设置随机化的选举超时时间
	r.electionTimeout = int(r.electionTimeout + int(randomOffset))
}

// Intn we should ensure the generation of random election timeout atomic
func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= uint64(r.electionTimeout)
}

func (r *raft) term(i uint64) (uint64, error) {
	// the valid term range is [index of dummy entry, last index]
	dummyIndex := r.ms.firstIndex() - 1
	// out of bounder
	if i < dummyIndex || i > r.ms.lastIndex() {
		// TODO: return an error instead?
		return 0, nil
	}
	t, err := r.ms.Term(i)
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

func getCommand(ent []*pb.Entry) []*applyEntry {
	result := make([]*applyEntry, 0, len(ent))
	for _, entry := range ent {
		data := string(entry.Data)
		parts := strings.Fields(data)
		applyEnt := &applyEntry{
			Command: parts[0],
			Key:     parts[1],
		}
		if len(parts) != 2 {
			applyEnt.Value = parts[2]
		}
		result = append(result, applyEnt)
	}
	return result
}

// sendTxnRequest Leader处理事务机制
func (r *raft) sendTxnRequest(msg *pb.Message) {
	TxnContextId := msg.TxnContextId
	TxnPackage := msg.TxnPackage
	Phase := pb.TransactionPhase_PHASE_PREPARE
	// 广播事务数据
	r.bcastTxnMessage(TxnContextId, TxnPackage, Phase)
}
func (r *raft) TxnSnapshotToMsg() *pb.Message {
	// 创建基础 Message
	msg := &pb.Message{
		Type: pb.MessageType_MsgCommitTxn, // 假设有对应的消息类型
		From: r.id,
		Term: r.Term,
	}

	// 如果 txnSnapshot 为空，返回空消息
	if r.txnSnapshot == nil {
		return msg
	}

	// 转换操作列表为 protobuf 的 Operation 数组
	operations := make([]*pb.TxnOperation, 0, len(r.txnSnapshot.Operations))
	for _, op := range r.txnSnapshot.Operations {
		pbOp := &pb.TxnOperation{
			Key:   string(op.Key),
			Value: op.Value,
		}
		// 设置操作类型
		switch op.Cmd {
		case "GET":
			pbOp.OpType = pb.OperationType_OP_READ
		case "PUT":
			pbOp.OpType = pb.OperationType_OP_PUT
		case "DELETE":
			pbOp.OpType = pb.OperationType_OP_DELETE
		}
		operations = append(operations, pbOp)
	}

	// 构建 TxnPackage
	txnPackage := &pb.TransactionPackage{
		Operations: operations,
	}
	// 设置到 Message 中
	msg.TxnPackage = txnPackage
	msg.TxnContextId = 0
	return msg
}

// 通过message将事务传入需要在raft层对事务进行解包
func (r *raft) constructTxnSnapshot(msg *pb.Message) {
	operations := msg.TxnPackage.Operations
	txnSnapshot := txn.NewTxnSnapshot()
	opList := make([]*txn.Operation, 0)
	conflictKeys := make(map[uint64]struct{})
	for _, op := range operations {
		if op.OpType == pb.OperationType_OP_READ {
			hashcode := txn.GenerateKeyHashCode([]byte(op.Key))
			tempOp := &txn.Operation{
				Cmd:   "GET",
				Key:   []byte(op.Key),
				Value: op.Value,
			}
			opList = append(opList, tempOp)
			conflictKeys[hashcode] = struct{}{}
			txnSnapshot.PendingReads[hashcode] = tempOp
		}
		if op.OpType == pb.OperationType_OP_PUT {
			hashcode := txn.GenerateKeyHashCode([]byte(op.Key))
			tempOp := &txn.Operation{
				Cmd:   "PUT",
				Key:   []byte(op.Key),
				Value: op.Value,
			}
			opList = append(opList, tempOp)
			txnSnapshot.PendingWrite[hashcode] = tempOp
		}
		if op.OpType == pb.OperationType_OP_DELETE {
			hashcode := txn.GenerateKeyHashCode([]byte(op.Key))
			tempOp := &txn.Operation{
				Cmd:   "DELETE",
				Key:   []byte(op.Key),
				Value: op.Value,
			}
			opList = append(opList, tempOp)
			txnSnapshot.PendingWrite[hashcode] = tempOp
		}
	}
	// 现在follower所拿到的时间是全局事务的开始时间
	txnSnapshot.StartWatermark = msg.StartTxnTime
	txnSnapshot.Operations = opList
	txnSnapshot.ConflictKeys = conflictKeys
	// 将日志信息保存在raft结构中
	r.txnSnapshot = txnSnapshot
	r.txnBuffer = msg.TxnPackage.Operations
	return
}

// follower对leader所发出的事务请求进行处理
func (r *raft) handleTxnRequest(msg *pb.Message) {
	// 将数据解包并保存在raft结构暂存区中
	r.constructTxnSnapshot(msg)
	checkList := make([]uint64, 0)
	for key, _ := range r.txnSnapshot.PendingReads {
		checkList = append(checkList, key)
	}
	// message需要将当前全局逻辑段也发送给follower
	GlobalLogicTime := r.txnSnapshot.StartWatermark
	CommitTs, _ := txn.GenerateHybridTs(GlobalLogicTime)
	log.Printf("node %d generate hybird timestamp %d", r.id, CommitTs)

	// 此时的提交时间是混合时钟
	r.txnSnapshot.CommitTime = CommitTs
	// 这里并不需要担心时钟偏移因为冲突的计算只会根据
	hasConflict, err := r.coordinatorClient.HandleConflictCheck(checkList, r.txnSnapshot.StartWatermark, r.txnSnapshot.CommitTime)
	if err != nil {
		log.Fatalf("node %d handle conflict check error: %v", r.id, err)
		return
	}

	if hasConflict {
		// todo 冲突处理
		log.Printf("conflict ! txn submit in follower %d\n", r.id)
		r.followerHandleConflict()
		return
	}
	// 将操作应用到状态机
	r.FollowerTxnCommit()
	// 构造回复信息
	msg = &pb.Message{
		Type: pb.MessageType_MsgCommitTxnResp,
		From: r.id,
		To:   r.lead,
	}
	log.Printf("follower %d send txn response message\n", r.id)
	r.send(msg)
}

// 如果follower在提交过程中发生了冲突，此时需要向leader进行仲裁
func (r *raft) followerHandleConflict() {
	// 这里直接将数据发送给Leader就好了，直接由Leader处理后续的数据处理
	msg := &pb.Message{
		From: r.id,
		To:   r.lead,
		Type: pb.MessageType_MsgCommitConflict,
	}
	// 通知leader当前的follower出现问题
	r.send(msg)
}

// todo Leader需要对冲突进行处理
func (r *raft) leaderHandleConflict() {
	log.Println("leader found conflict!")
}

//func (r *raft) Commit() error {
//	checkList := make([]uint64, 0)
//	for key, _ := range r.txnSnapshot.ConflictKeys {
//		checkList = append(checkList, key)
//	}
//	// 此处的CommitTs是follower的提交时间，
//	commitTs := txn.GetCurrenTime()
//	hasConflict, err := r.coordinatorClient.HandleConflictCheck(checkList, r.txnSnapshot.StartWatermark, commitTs)
//	if err != nil {
//		return err
//	}
//	if hasConflict {
//		r.followerHandleConflict()
//		return nil
//	}
//	// 遍历事务的operations并执行
//	// 这里需要加锁
//	applyEntryList := make([]*applyEntry, 0)
//	for _, operation := range r.txnSnapshot.Operations {
//		applyEntry := &applyEntry{
//			Command: operation.Cmd,
//			Key:     string(operation.Key),
//			Value:   string(operation.Value),
//		}
//		applyEntryList = append(applyEntryList, applyEntry)
//	}
//	r.app.applyc <- applyEntryList
//	return nil
//}

// todo leader需要如何将数据返回，是后续需要改进的点
// TxnCommit 这个函数的Commit的本质其实也只是将操作变为对应的日志的内容，也就是AppendEntry，由协程自动的将数据应用到状态机中
func (r *raft) LeaderTxnCommit() [][]byte {
	//ReadingResults := make([][]byte, 0)
	if r.state == StateLeader {
		log.Printf("leader %d start committing ...\n", r.id)
	}
	//readLen := len(r.txnSnapshot.PendingReads)
	operations := r.txnBuffer
	// 根据operations中的内容按照之前规定的Entry的格式进行添加
	entrys := make([]*pb.Entry, 0)
	for _, operation := range operations {
		cmd := operation.OpType
		key := string(operation.Key)
		value := string(operation.Value)
		detailCmd := ""
		switch cmd {
		case pb.OperationType_OP_PUT:
			detailCmd = fmt.Sprintf("PUT %s %s", key, value)
		case pb.OperationType_OP_DELETE:
			detailCmd = fmt.Sprintf("DELETE %s", key)
		case pb.OperationType_OP_READ:
			detailCmd = fmt.Sprintf("GET %s %s", key, value)
		}
		tempEntry := &pb.Entry{
			Term:  r.Term,
			Index: r.raftLog.lastIndex(),
			Data:  []byte(detailCmd),
		}
		entrys = append(entrys, tempEntry)
	}
	// 应用日志
	r.appendEntry(entrys)
	r.app.commitc <- entrys
	// 应用到状态机上
	r.app.applyc <- getCommand(entrys)
	if r.state == StateLeader {
		log.Printf("leader %d finished committed\n", r.id)
	}
	// todo 有可能这个地方卡住了，现在就是这个功能的问题了，进程没法往下走了
	//if readLen != 0 {
	//	content := <-r.app.resultc
	//	ReadingResults = append(ReadingResults, content)
	//	return ReadingResults
	//}
	log.Printf("node %d,state %d commit the txn!\n", r.id, r.state)
	return nil
}
func (r *raft) FollowerTxnCommit() [][]byte {
	if r.state == StateLeader {
		log.Printf("leader %d start committing ...\n", r.id)
	}
	operations := r.txnBuffer
	// 根据operations中的内容按照之前规定的Entry的格式进行添加
	entrys := make([]*pb.Entry, 0)
	for _, operation := range operations {
		cmd := operation.OpType
		key := string(operation.Key)
		value := string(operation.Value)
		detailCmd := ""
		switch cmd {
		case pb.OperationType_OP_PUT:
			detailCmd = fmt.Sprintf("PUT %s %s", key, value)
		case pb.OperationType_OP_DELETE:
			detailCmd = fmt.Sprintf("DELETE %s", key)
		case pb.OperationType_OP_READ:
			detailCmd = fmt.Sprintf("GET %s %s", key, value)
		}
		tempEntry := &pb.Entry{
			Term:  r.Term,
			Index: r.raftLog.lastIndex(),
			Data:  []byte(detailCmd),
		}
		entrys = append(entrys, tempEntry)
	}
	// 应用日志
	r.appendEntry(entrys)
	r.app.commitc <- entrys
	// 应用到状态机上
	r.app.applyc <- getCommand(entrys)
	if r.state == StateLeader {
		log.Printf("leader %d finished committed\n", r.id)
	}
	log.Printf("node %d,state %d commit the txn!\n", r.id, r.state)
	return nil
}
func (r *raft) TxnGetTimestamp() (uint64, error) {
	hyTs, err := txn.GenerateHybridTs(r.logicalTs)
	if err != nil {
		return 0, err
	}
	return hyTs, nil
}

// handleTxnCommitResp Leader处理消息回复
// 需要在进行合法性仲裁的同时进行Commit
func (r *raft) handleTxnCommitResp(msg *pb.Message) error {
	// 记录当前投票的follower
	log.Printf("leader %d get txn commit resp from %d", r.id, msg.From)
	r.processTracker.TxnCommitted[msg.From] = true
	poll := r.TxnPoll()
	// 对投票情况进行仲裁
	if poll {
		// 将自身的事务快照提交并将其保存在zk中(注，快照需要保存在全局配置中心中)
		err := r.coordinatorClient.SaveSnapshot(r.txnSnapshot)
		log.Printf("leader %d save the txn snapshot to coordinator\n", r.id)
		if err != nil {
			log.Fatalln(err)
			return err
		}

		checkList := make([]uint64, 0)
		for key, _ := range r.txnSnapshot.ConflictKeys {
			checkList = append(checkList, key)
		}

		// 此处的CommitTs是follower的提交时间，
		commitTs, err := txn.GenerateHybridTs(r.logicalTs)
		log.Printf("[leader] node %d generate hybird timestamp %d", r.id, commitTs)
		if err != nil {
			return err
		}
		// 将混合时钟放入水位线中
		r.coordinator.WaterMark.AddCommitTime(commitTs)
		hasConflict, err := r.coordinatorClient.HandleConflictCheck(checkList, r.txnSnapshot.StartWatermark, commitTs)
		if err != nil {
			return err
		}
		if !hasConflict {
			log.Printf("no conflict ! txn submit in leader %d\n", r.id)
			content := r.LeaderTxnCommit()
			// 把这个内容放到当前leader的缓冲区中
			// 指针直接指向这个内容
			r.txnContent = content
		} else {
			// 进入事务冲突处理部分
			log.Printf("conflict ! txn submit in leader %d\n", r.id)
			r.leaderHandleConflict()
		}
	}
	return nil
}

func (r *raft) TxnPoll() bool {
	majorityCount := len(r.processTracker.TxnCommitted)/2 + 1
	Committed := 0
	for _, committed := range r.processTracker.TxnCommitted {
		if committed {
			Committed++
		}
	}
	if Committed >= majorityCount {
		return true
	} else {
		return false
	}
}

// 专门用于Leader处理 todo 这里会涉及到回滚和幂等性的问题等框架成功运行起来之后再来继续搞
func (r *raft) leaderConflictHandler() {
	log.Println("Leader has caught the conflict message!")
}
