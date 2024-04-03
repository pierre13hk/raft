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

type InMemoryRaftRPC struct {
	Peers map[uint64]*Node
}

func NewInMemoryRaftRPC() *InMemoryRaftRPC {
	return &InMemoryRaftRPC{
		Peers: make(map[uint64]*Node),
	}
}

func (r *InMemoryRaftRPC) RequestVoteRPC(p Peer, ballot Ballot, c chan BallotResponse) error {
	peer := r.Peers[p.Id]
	if peer == nil {
		return errors.New("Peer not found")
	}

	// Simulate network latency
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

	defer func() {
		if recover() != nil {
			// we wrote to a closed channel
			// the election is over.
		}
	}()
	resp := peer.RequestVote(ballot)
	c <- resp
	return nil
}

func (r *InMemoryRaftRPC) AppendEntriesRPC(p Peer, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	peer := r.Peers[p.Id]
	if peer == nil {
		return AppendEntriesResponse{}, errors.New("Peer not found")
	}

	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	// Simulate network latency

	resp := peer.recvAppendEntries(req)
	return resp, nil
}
