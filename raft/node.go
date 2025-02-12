package raft

import "ComDB/raft/pb"

// Node the bridge of application(ComDB) and raft module
type Node interface {
}

type Ready struct {
	Entries          []*pb.Entry   // 需要持久化的新日志条目
	CommittedEntries []*pb.Entry   // 已提交待应用的日志条目
	Messages         []*pb.Message // 需要发送给其他节点的消息
	HardState        HardState     // 需要持久化的硬状态（Term、Vote、Commit Index）
	SoftState        SoftState     // 软状态（Leader ID、节点角色）
}
type node struct {
	// a lot of channels
	readyc   chan Ready    // 传递待处理操作的通道
	advancec chan struct{} // 通知处理完成的通道
	raft     *RawNode      // 底层Raft状态机
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt HardState) Ready {
	rd := Ready{
		//
		Entries:          r.ms.ents,
		CommittedEntries: r.raftLog.nextEnts(),
		Messages:         r.msgs,
	}

	// 检查软状态是否发生变化
	if softSt := r.softState(); softSt != nil && (prevSoftSt == nil || !softSt.equal(prevSoftSt)) {
		rd.SoftState = *softSt
	}
	// 检查硬状态是否发生变化
	if hardSt := r.hardState(); !isHardStateEqual(*hardSt, prevHardSt) {
		rd.HardState = *hardSt
	}
	return rd
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

func isHardStateEqual(a, b HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}
