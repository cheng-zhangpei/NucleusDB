package raft

import "ComDB"

// this is the application that communicate with node
// here is another go routine
type persistent struct {
	// db ComDB instance
	db ComDB.DB
	// bind 2 channels
	node node
}
