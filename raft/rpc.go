package raft

import (
	"errors"
	//"fmt"
	"math/rand"
	"time"
)

type Peer struct {
	Id uint64
}

type RaftRPC interface {
	RequestVoteRPC(peer Peer, ballot Ballot, c chan BallotResponse) error
	AppendEntriesRPC(peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error)
}