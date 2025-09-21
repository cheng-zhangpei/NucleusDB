package configchange

import "NucleusDB/raft"

type JoinConfig struct {
	oldConfig raft.RaftConfig
	newConfig raft.RaftConfig
}
