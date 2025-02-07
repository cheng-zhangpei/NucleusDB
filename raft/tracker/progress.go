package tracker

// Progress 代表单个节点的日志复制进度和状态。
// 每个节点都会维护自身的进度信息。
type Progress struct {
	// Match 已知匹配的最高日志索引，表示跟随者已成功复制到该索引的日志条目。
	Match uint64
	// Next 下一个将发送给该跟随者（或学习者）的日志条目索引。
	Next uint64
	// RecentActive 表示该节点最近是否活跃，从节点发送任何消息时会被设置为 true。
	RecentActive bool
	// ProbeSent 在 StateProbe 状态下，表示已发送探测消息，直到重置前暂停发送新消息。
	ProbeSent bool
}
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// BecomeFollower 将节点状态切换为 Follower（跟随者）。
// Follower 会响应 Leader 的心跳和日志复制请求。
func (pr *Progress) BecomeFollower() {}

// BecomeCandidate 将节点状态切换为 Candidate（候选人）。
// Candidate 会发起选举，请求其他节点的投票。
func (pr *Progress) BecomeCandidate() {}

// BecomeLeader 将节点状态切换为 Leader（领导者）。
// Leader 负责管理日志复制和发送心跳。
func (pr *Progress) BecomeLeader() {}

// SendHeartbeat Leader 向所有跟随者发送心跳消息（AppendEntries 请求）。
// 心跳消息用于维持 Leader 和 Follower 之间的连接，并防止选举超时。
func (pr *Progress) SendHeartbeat() {}

// HandleAppendEntriesResponse 处理跟随者对心跳消息的响应。
// Leader 根据响应结果调整日志复制策略。
//func (pr *Progress) HandleAppendEntriesResponse(response *AppendEntriesResponse){}

// HandleVoteRequest 处理选举请求（RequestVote）。
// Candidate 发起选举时，其他节点会响应此请求。
//func (pr *Progress) HandleVoteRequest(request *VoteRequest) *VoteResponse

// HandleVoteResponse 处理选举响应（RequestVote 的响应）。
// Candidate 收到选举响应后，根据结果决定是否当选。
//func (pr *Progress) HandleVoteResponse(response *VoteResponse)

// StartElection 启动选举过程。
// 当节点的状态变为 Candidate 时，会发起选举。
func (pr *Progress) StartElection() {}

// ResetElectionTimer 重置选举超时计时器，防止重复选举。
// 心跳消息或选举响应会重置该计时器。
func (pr *Progress) ResetElectionTimer() {}

// UpdateCommitIndex 更新已提交的日志条目索引。
// Leader 根据匹配的日志条目数量确定新的已提交索引。
func (pr *Progress) UpdateCommitIndex() {}

// ApplyLogEntries 将已提交的日志条目应用到状态机。
// 已提交的日志条目会被持久化并应用于状态机。
func (pr *Progress) ApplyLogEntries() {}

// MaybeUpdate 处理跟随者的确认响应，更新日志条目索引。
// 当跟随者确认收到日志条目时，Leader 会更新相关索引。
func (pr *Progress) MaybeUpdate(n uint64) bool { return false }

// MaybeDecrTo 处理跟随者的拒绝响应，调整日志条目索引。
// 当跟随者拒绝接收日志条目时，Leader 会调整发送策略。
func (pr *Progress) MaybeDecrTo(rejected, matchHint uint64) bool { return false }

// OptimisticUpdate 乐观地更新日志条目索引，用于快速复制日志。
// Leader 在发送一批日志条目后，乐观地更新索引以加快复制速度。
func (pr *Progress) OptimisticUpdate(n uint64) {}

// IsPaused 检查当前节点是否处于暂停状态。
// 暂停状态表示节点暂时不接受新的日志条目。
func (pr *Progress) IsPaused() bool { return false }

// String 提供 Progress 的字符串表示，方便调试和日志记录。
// 字符串输出包含节点的状态、索引等信息。
func (pr *Progress) String() string { return "" }

// ResetState 重置 Progress 的状态和相关字段。
// 在状态切换或其他状态变化时调用，确保状态的一致性。
func (pr *Progress) ResetState(state StateType) {}
