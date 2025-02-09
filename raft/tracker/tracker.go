package tracker

import (
	"fmt"
	"sort"
)

// ProgressMap 是一个映射，用于存储每个节点的进度信息。
type ProgressMap map[uint64]*Progress

// ProgressTracker 跟踪 Raft 集群的进度和投票信息。(executed by leader) progressTracker(leader) -> progress(followers)
type ProgressTracker struct {
	Progress ProgressMap     // 各节点的进度
	Votes    map[uint64]bool // 投票信息
}

// MakeProgressTracker 初始化一个 ProgressTracker 实例。
func MakeProgressTracker() *ProgressTracker {
	return &ProgressTracker{
		Progress: make(ProgressMap),
		Votes:    make(map[uint64]bool),
	}
}

// AddNode 添加一个新节点到 ProgressTracker 中。
func (pt *ProgressTracker) AddNode(id uint64) {
	pt.Progress[id] = &Progress{
		Match:         0,
		Next:          1,
		RecentActive:  true,
		ProbeSent:     false,
		State:         StateFollower,
		ElectionReset: false,
	}
}

// RemoveNode 从 ProgressTracker 中移除一个节点。
func (pt *ProgressTracker) RemoveNode(id uint64) {
	delete(pt.Progress, id)
}

// GetQuorumSize 获取形成仲裁所需的最小节点数。
func (pt *ProgressTracker) GetQuorumSize() int {
	count := 0
	for _, _ = range pt.Progress {
		count++
	}
	return (count / 2) + 1
}

// QuorumActive 判断仲裁Node是否活跃。
func (pt *ProgressTracker) QuorumActive() bool {
	active := 0
	for _, pr := range pt.Progress {
		// judge if the node is active
		if pr.RecentActive {
			active++
		}
	}
	return active >= pt.GetQuorumSize()
}

// RecordVote 记录一个节点的投票。
func (pt *ProgressTracker) RecordVote(id uint64, vote bool) {
	// update in pt
	pt.Votes[id] = vote
}

// TallyVotes 统计投票结果。
func (pt *ProgressTracker) TallyVotes() (granted, rejected int) {
	for id, _ := range pt.Progress {
		vote, ok := pt.Votes[id]
		if !ok {
			continue
		}
		if vote {
			granted++
		} else {
			rejected++
		}
	}
	return
}

// Committed 返回最高的已提交日志索引。
func (pt *ProgressTracker) Committed() uint64 {
	var matches []uint64
	for _, pr := range pt.Progress {
		matches = append(matches, pr.Match)
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i] < matches[j]
	})
	if len(matches) == 0 {
		return 0
	}
	return matches[len(matches)/2]
}

// String 返回 ProgressTracker 的字符串表示。
func (pt *ProgressTracker) String() string {
	var buf string
	buf += "Progress:\n"
	for id, pr := range pt.Progress {
		buf += fmt.Sprintf("  Node %d: Match=%d, Next=%d, State=%d, Active=%v\n",
			id, pr.Match, pr.Next, pr.State, pr.RecentActive)
	}
	buf += "Votes:\n"
	for id, vote := range pt.Votes {
		buf += fmt.Sprintf("  Node %d: %v\n", id, vote)
	}
	return buf
}

// ResetElectionTimer 重置选举计时器。
func (pt *ProgressTracker) ResetElectionTimer() {
	for _, pr := range pt.Progress {
		pr.ElectionReset = true
	}
}

// UpdateCommitIndex 更新提交索引。
func (pt *ProgressTracker) UpdateCommitIndex() {
	committed := pt.Committed()
	for _, pr := range pt.Progress {
		if pr.State == StateLeader {
			pr.Match = committed
		}
	}
}

// use Func(user give) to visit node(progress)
func (p *ProgressTracker) Visit(f func(id uint64, pr *Progress)) {
	n := len(p.Progress)
	// We need to sort the IDs and don't want to allocate since this is hot code.
	// The optimization here mirrors that in `(MajorityConfig).CommittedIndex`,
	// see there for details.
	var sl [7]uint64

	var ids []uint64
	if len(sl) >= n {
		ids = sl[:n]
	} else {
		ids = make([]uint64, n)
	}
	for id := range p.Progress {
		n--
		ids[n] = id
	}
	insertionSort(ids)
	for _, id := range ids {
		f(id, p.Progress[id])
	}
}

func insertionSort(sl []uint64) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}
