package raft

import "ComDB/raft/pb"

//	func limitSize(ents []pb.Entry, maxSize uint64) []pb.Entry {
//		if len(ents) == 0 {
//			return ents
//		}
//		size := ents[0].Size()
//		var limit int
//		for limit = 1; limit < len(ents); limit++ {
//			size += ents[limit].Size()
//			if uint64(size) > maxSize {
//				break
//			}
//		}
//		return ents[:limit]
//	}
func IsLocalMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgHup || msgt == pb.MessageType_MsgBeat
}
func IsResponseMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgAppResp || msgt == pb.MessageType_MsgVoteResp || msgt == pb.MessageType_MsgHeartbeatResp
}
func PayloadSize(e *pb.Entry) int {
	return len(e.Data)
}
