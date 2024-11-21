package raft

import (
	"testing"
)

var (
	defaultNodeConfig = NodeConfig{
		HeartbeatTimeout:   100,
		ElectionTimeoutMin: 300,
		ElectionTimeoutMax: 500,
	}
)

func testNode(t *testing.T) *Node {
	return NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir(), defaultNodeConfig)
}

func TestCheckVoteRequestGranted(t *testing.T) {
	node := testNode(t)

	b := Ballot{
		Term:         0,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node.checkVoteRequest(b)
	if resp.VoteGranted {
		t.Errorf("Expected vote not granted")
	}
}

func TestCheckVoteRequestBallotLate(t *testing.T) {
	// Test that we don't grant a vote if our term is greater
	node := testNode(t)
	node.state.currentTerm = 2
	b := Ballot{
		Term:         1,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp := node.checkVoteRequest(b)
	if resp.VoteGranted {
		t.Errorf("Expected vote not granted")
	}
}

func TestCheckVoteRequestAlreadyVoted(t *testing.T) {
	// Test that we don't grant a vote if we already voted
	node := testNode(t)
	node.state.currentTerm = 1
	node.state.votedFor = 3
	b := Ballot{
		Term:         2,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp := node.checkVoteRequest(b)
	if resp.VoteGranted {
		t.Errorf("Expected vote not granted")
	}
}

func TestCheckVoteRequestLogTermLate(t *testing.T) {
	// Test that we don't grant a vote if the candidate's log's term is lesser
	var CURRENT_TERM uint64 = 3
	var CANDINDATE_TERM uint64 = 2
	node := testNode(t)
	node.state.currentTerm = CURRENT_TERM
	node.state.votedFor = 0
	entries := []LogEntry{
		{
			Term:    1,
			Index:   0,
			Type:    RAFT_LOG,
			Command: []byte("test"),
		},
		{
			Term:    CURRENT_TERM,
			Index:   2,
			Type:    USER_LOG,
			Command: []byte("user command"),
		},
	}
	node.state.Logger.Append(entries)
	b := Ballot{
		Term:         CANDINDATE_TERM,
		CandidateId:  2,
		LastLogIndex: 2,
		LastLogTerm:  2,
	}
	resp := node.checkVoteRequest(b)
	if resp.VoteGranted {
		t.Errorf("Expected vote not granted")
	}
}

func TestCheckVoteRequestLogIndexLate(t *testing.T) {
	// Test that we don't grant a vote if the candidate's log's index is lesser
	node := testNode(t)
	node.state.currentTerm = 2
	node.state.votedFor = 0
	entries := []LogEntry{
		{
			Term:    1,
			Index:   0,
			Type:    RAFT_LOG,
			Command: []byte("init"),
		},
		{
			Term:    2,
			Index:   2,
			Type:    USER_LOG,
			Command: []byte("user command"),
		},
	}
	node.state.Logger.Append(entries)
	b := Ballot{
		Term:         2,
		CandidateId:  2,
		LastLogIndex: 1,
		LastLogTerm:  2,
	}
	resp := node.checkVoteRequest(b)
	if resp.VoteGranted {
		t.Errorf("Expected vote not granted")
	}
}
