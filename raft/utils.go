package raft

//
//func limitSize(ents []pb.Entry, maxSize uint64) []pb.Entry {
//	if len(ents) == 0 {
//		return ents
//	}
//	size := ents[0].Size()
//	var limit int
//	for limit = 1; limit < len(ents); limit++ {
//		size += ents[limit].Size()
//		if uint64(size) > maxSize {
//			break
//		}
//	}
//	return ents[:limit]
//}
