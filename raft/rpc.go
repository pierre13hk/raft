package raft

import (
	"errors"
	"fmt"
)

type Peer struct {
	Id uint64
}

type RaftRPC interface {
	RequestVoteRPC(peer Peer, ballot Ballot, c chan BallotResponse) error
	AppendEntriesRPC(peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

type InMemoryRaftRPC struct {
	peers map[uint64]*Node
}

func NewInMemoryRaftRPC() *InMemoryRaftRPC {
	return &InMemoryRaftRPC{
		peers: make(map[uint64]*Node),
	}
}

func (r *InMemoryRaftRPC) RequestVoteRPC(p Peer, ballot Ballot, c chan BallotResponse) error {
	peer := r.peers[p.Id]
	if peer == nil {
		return errors.New("Peer not found")
	}

	// Simulate network latency

	defer func() {
		if recover() != nil {
			// we wrote to a closed channel
			// the election is over.
			fmt.Println("Wrote to closed channel. Election over.")
		}
	}()
	resp := peer.RequestVote(ballot)
	c <- resp
	return nil
}

func (r *InMemoryRaftRPC) AppendEntriesRPC(p Peer, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	peer := r.peers[p.Id]
	if peer == nil {
		return AppendEntriesResponse{}, errors.New("Peer not found")
	}

	// Simulate network latency

	resp := peer.AppendEntries(req)
	return resp, nil
}
