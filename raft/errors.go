package raft

import "errors"

// Public errors
var (
	NodeNotLeaderErr = errors.New("node is not the leader")
)