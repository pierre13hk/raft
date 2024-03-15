package raft

import (
	"errors"
)
type Peer struct {
	Id uint64
}

type RaftRPC interface {
	RequestVoteRPC(peer Peer, ballot Ballot) (BallotResponse, error)
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

func (r *InMemoryRaftRPC) RequestVoteRPC(p Peer, ballot Ballot) (BallotResponse, error) {
	peer := r.peers[p.Id]
	if peer == nil {
		return BallotResponse{}, errors.New("Peer not found")
	}

	// Simulate network latency
	
	resp := peer.RequestVote(ballot)
	return resp, nil
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

