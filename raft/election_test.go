package raft

import (
	"testing"
	//"time"
)

func TestRequestVote(t *testing.T) {
	node1 := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir(), NodeConfig{})
	node2 := NewNode(2, "localhost:1235", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir(), NodeConfig{})
	node3 := NewNode(3, "localhost:1236", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir(), NodeConfig{})

	node1.Peers = []Peer{{Id: 2}, {Id: 3}}

	rpc := NewInMemoryRaftRPC()
	rpc.Peers = map[uint64]*Node{
		1: node1,
		2: node2,
		3: node3,
	}

	node1.RaftRPC = rpc
	node2.RaftRPC = rpc
	node3.RaftRPC = rpc

	b := Ballot{
		Term:         0,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node1.checkVoteRequest(b)
	if resp.VoteGranted {
		t.Errorf("Expected vote not granted")
	}
}
