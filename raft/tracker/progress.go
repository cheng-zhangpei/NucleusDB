package tracker

import (
	"fmt"
	_ "sort"
)

// Progress 代表单个节点的日志复制进度和状态。
type Progress struct {
	Match        uint64 // 已知匹配的最高日志索引
	Next         uint64 // 下一个将发送给该跟随者（或学习者）的日志条目索引
	RecentActive bool   // 表示该节点最近是否活跃
	ProbeSent    bool   // 表示是否已发送探测消息

	State         StateType // 节点当前状态
	ElectionReset bool      // 标记选举超时计时器是否已被重置
}

// StateType 表示节点的 Raft 状态。
type StateType uint64

const (
	StateFollower  StateType = iota // 跟随者状态
	StateCandidate                  // 候选人状态
	StateLeader                     // 领导者状态
)

// BecomeFollower 将节点状态切换为 Follower（跟随者）。
func (pr *Progress) BecomeFollower() {
	pr.State = StateFollower
	pr.ResetState()
}

// BecomeCandidate 将节点状态切换为 Candidate（候选人）。
func (pr *Progress) BecomeCandidate() {
	pr.State = StateCandidate
	pr.ResetState()
}

// BecomeLeader 将节点状态切换为 Leader（领导者）。
func (pr *Progress) BecomeLeader() {
	pr.State = StateLeader
	pr.ResetState()
}

// SendHeartbeat Leader 向所有跟随者发送心跳消息（AppendEntries 请求）。
func (pr *Progress) SendHeartbeat() {
	// 实现心跳消息发送逻辑
}

// HandleAppendEntriesResponse 处理跟随者对心跳消息的响应。
func (pr *Progress) HandleAppendEntriesResponse(n uint64) bool {
	return pr.MaybeUpdate(n)
}

// HandleVoteRequest 处理选举请求（RequestVote）。
func (pr *Progress) HandleVoteRequest() {
	// 实现选举请求处理逻辑
}

// HandleVoteResponse 处理选举响应（RequestVote 的响应）。
func (pr *Progress) HandleVoteResponse(vote bool) {
	// 实现选举响应处理逻辑
}

// StartElection 启动选举过程。
func (pr *Progress) StartElection() {
	// 实现选举启动逻辑
}

// ResetElectionTimer 重置选举超时计时器。
func (pr *Progress) ResetElectionTimer() {
	pr.ElectionReset = true
}

// UpdateCommitIndex 更新已提交的日志条目索引。
func (pr *Progress) UpdateCommitIndex() {
	// 实现提交索引更新逻辑
}

// ApplyLogEntries 将已提交的日志条目应用到状态机。
func (pr *Progress) ApplyLogEntries() {
	// 实现日志条目应用逻辑
}

// MaybeUpdate 处理跟随者的确认响应，更新日志条目索引。
func (pr *Progress) MaybeUpdate(n uint64) bool {
	if pr.Match < n {
		pr.Match = n
		pr.Next = n + 1
		return true
	}
	return false
}

// MaybeDecrTo 处理跟随者的拒绝响应，调整日志条目索引。
func (pr *Progress) MaybeDecrTo(rejected, matchHint uint64) bool {
	if pr.Next-1 != rejected {
		return false
	}
	pr.Next = max(min(rejected, matchHint+1), 1)
	return true
}

// OptimisticUpdate 乐观地更新日志条目索引。
func (pr *Progress) OptimisticUpdate(n uint64) {
	pr.Next = n + 1
}

// IsPaused 检查当前节点是否处于暂停状态。
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case StateFollower:
		return false
	case StateCandidate:
		return false
	case StateLeader:
		return false
	default:
		return true
	}
}

// String 提供 Progress 的字符串表示。
func (pr *Progress) String() string {
	return fmt.Sprintf("State=%d Match=%d Next=%d Active=%v", pr.State, pr.Match, pr.Next, pr.RecentActive)
}

// ResetState 重置 Progress 的状态和相关字段。
func (pr *Progress) ResetState() {
	pr.Next = pr.Match + 1
	pr.RecentActive = false
	pr.ProbeSent = false
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
