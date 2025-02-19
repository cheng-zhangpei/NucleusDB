package raft

import (
	"ComDB"
	"ComDB/raft/pb"
)

// this is the application that communicate with node
// here is another go routine
type applyEntry struct {
	command string
	key     string
	value   string
}
type application struct {
	DB ComDB.DB
	// 将需要应用的内容发送给上层的通道
	applyc chan *applyEntry
	// 日志持久化的通道
	commitc chan *pb.Entry
}
