package raft

import (
	"testing"
	"time"
)

func TestRequestVote(t *testing.T) {

	node1 := NewNode(1, "localhost:1234", NewInMemoryRaftRPC())
	node2 := NewNode(2, "localhost:1235", NewInMemoryRaftRPC())
	node3 := NewNode(3, "localhost:1236", NewInMemoryRaftRPC())

	node1.Peers = []Peer{{Id: 2}, {Id: 3}}
	node2.Peers = []Peer{{Id: 1}, {Id: 3}}
	node3.Peers = []Peer{{Id: 1}, {Id: 2}}

	rpc := NewInMemoryRaftRPC()
	rpc.Peers = map[uint64]*Node{
		1: node1,
		2: node2,
		3: node3,
	}

	node1.RaftRPC = rpc
	node2.RaftRPC = rpc
	node3.RaftRPC = rpc

	node1.StartElection()
	b := Ballot{
		Term:         0,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node1.HandleVoteRequest(b)
	if resp.VoteGranted {
		t.Errorf("Expected vote not granted")
	}
}

func TestStartElection(t *testing.T) {
	node1 := NewNode(1, "localhost:1234", NewInMemoryRaftRPC())
	node2 := NewNode(2, "localhost:1234", NewInMemoryRaftRPC())
	node3 := NewNode(3, "localhost:1234", NewInMemoryRaftRPC())
	node4 := NewNode(4, "localhost:1234", NewInMemoryRaftRPC())
	node5 := NewNode(5, "localhost:1234", NewInMemoryRaftRPC())

	node1.Peers = []Peer{{Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}}
	node2.Peers = []Peer{{Id: 1}, {Id: 3}, {Id: 4}, {Id: 5}}
	node3.Peers = []Peer{{Id: 1}, {Id: 2}, {Id: 4}, {Id: 5}}
	node4.Peers = []Peer{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 5}}
	node5.Peers = []Peer{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}}

	rpc := NewInMemoryRaftRPC()
	rpc.Peers = map[uint64]*Node{
		1: node1,
		2: node2,
		//3: node3,
		//4: node4,
		//5: node5,
	}

	node1.RaftRPC = rpc
	node2.RaftRPC = rpc
	node3.RaftRPC = rpc
	node4.RaftRPC = rpc
	node5.RaftRPC = rpc

	node1.Start()
	node1.StartElection()
	time.Sleep(1 * time.Second)
	if node1.role != Candidate {
		t.Errorf("Expected role to be Candidate, was %s", node1.role.String())
	}

	rpc.Peers = map[uint64]*Node{
		1: node1,
		2: node2,
		3: node3,
		4: node4,
		5: node5,
	}

	go node1.StartElection()
	time.Sleep(1 * time.Second)
	if node1.role != Leader {
		t.Errorf("Expected role to be Leader, was %s", node1.role.String())
	}
}

func TestStartElectionRetry(t *testing.T) {
	node1 := NewNode(1, "localhost:1234", NewInMemoryRaftRPC())
	node2 := NewNode(2, "localhost:1234", NewInMemoryRaftRPC())
	node3 := NewNode(3, "localhost:1234", NewInMemoryRaftRPC())
	node4 := NewNode(4, "localhost:1234", NewInMemoryRaftRPC())
	node5 := NewNode(5, "localhost:1234", NewInMemoryRaftRPC())

	node1.Peers = []Peer{{Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}}
	node2.Peers = []Peer{{Id: 1}, {Id: 3}, {Id: 4}, {Id: 5}}
	node3.Peers = []Peer{{Id: 1}, {Id: 2}, {Id: 4}, {Id: 5}}
	node4.Peers = []Peer{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 5}}
	node5.Peers = []Peer{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}}

	rpc := NewInMemoryRaftRPC()
	rpc.Peers = map[uint64]*Node{
		1: node1,
		2: node2,
		//3: node3,
		//4: node4,
		//5: node5,
	}

	node1.RaftRPC = rpc
	node2.RaftRPC = rpc
	node3.RaftRPC = rpc
	node4.RaftRPC = rpc
	node5.RaftRPC = rpc

	go node1.StartElection()
	time.Sleep(200 * time.Millisecond)
	if node1.role != Candidate {
		t.Errorf("Expected role to be Candidate, was %s", node1.role.String())
	}

	rpc.Peers = map[uint64]*Node{
		1: node1,
		2: node2,
		3: node3,
		4: node4,
		5: node5,
	}

}

func TestCandidateBeaten(t *testing.T) {
	node := NewNode(1, "localhost:1234", NewInMemoryRaftRPC())
	node.state.currentTerm = 1
	node.role = Candidate
	node.state.votedFor = 1

	b := Ballot{
		Term:         3,
		CandidateId:  2,
		LastLogIndex: 1,
		LastLogTerm:  1,
	}

	resp := node.HandleVoteRequest(b)
	if !resp.VoteGranted {
		t.Errorf("Expected vote granted")
	}

	if node.role != Follower {
		t.Errorf("Expected role to be Follower, was %s", node.role.String())
	}

	if node.state.votedFor != 2 {
		t.Errorf("Expected votedFor to be 2, was %d", node.state.votedFor)
	}
}
