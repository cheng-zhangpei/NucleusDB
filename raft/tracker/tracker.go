package tracker

// 1. Quorum activity check.  2. Collect the vote status of the cluster

type ProgressMap map[uint64]*Progress
type ProgressTracker struct {
	Progress ProgressMap

	Votes map[uint64]bool

	MaxInflight int
}
