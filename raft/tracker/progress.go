package tracker

import (
	"fmt"
	_ "sort"
)

// Progress 代表单个节点的日志复制进度和状态。
type Progress struct {
	Match            uint64 // 已知匹配的最高日志索引
	Next             uint64 // 下一个将发送给该跟随者（或学习者）的日志条目索引
	RecentActive     bool   // 表示该节点最近是否活跃
	ProbeSent        bool   // 表示是否已发送探测消息
	UncertainMessage *Inflights
	State            StateType // 节点当前状态
	ElectionReset    bool      // 标记选举超时计时器是否已被重置
}

func NewProgress(maxFlightSize int) *Progress {
	return &Progress{
		Match:            0,                           // 初始时没有匹配的日志条目
		Next:             1,                           // 初始时下一个要发送的日志条目索引
		RecentActive:     false,                       // 初始时节点不活跃
		ProbeSent:        false,                       // 初始时未发送探测消息
		UncertainMessage: newInflights(maxFlightSize), // 初始时没有不确定的消息
		State:            StateReplicate,              // 初始状态为未冲突
		ElectionReset:    false,                       // 初始时选举超时计时器未被重置
	}
}

// BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
// optionally and if larger, the index of the pending snapshot.
func (pr *Progress) BecomeProbe() {
	// If the original state is StateSnapshot, progress knows that
	pr.ResetState(StateProbe)
	pr.Next = pr.Match + 1

}

// BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
func (pr *Progress) BecomeReplicate() {
	pr.ResetState(StateReplicate)
	pr.Next = pr.Match + 1
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

// MaybeUpdate 处理跟随者的确认响应，更新日志条目索引。说人话就是记录现在的follower匹配到啥位置了
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

// String 提供 Progress 的字符串表示。
func (pr *Progress) String() string {
	return fmt.Sprintf("State=%d Match=%d Next=%d Active=%v", pr.State, pr.Match, pr.Next, pr.RecentActive)
}

// ResetState 重置 Progress 的状态和相关字段。
func (pr *Progress) ResetState(state StateType) {
	pr.ProbeSent = false
	pr.State = state
	pr.UncertainMessage.reset()

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
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case StateProbe:
		return pr.ProbeSent
	case StateReplicate:
		return pr.UncertainMessage.Full()
	case StateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}
