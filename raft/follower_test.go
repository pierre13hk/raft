package raft

import (
	"fmt"
	"testing"
)

func TestCheckAppendEntriesTermLate(t *testing.T) {
	// Test that we don't accept an append entries request if the term less than our current term
	node := testNode(t)
	node.state.currentTerm = 2
	req := AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	resp := node.checkAppendEntriesRequest(req)
	if resp.Success {
		t.Errorf("Expected entries to be refused")
	}
}

func TestCheckAppendEntriesCandidateBeaten(t *testing.T) {
	// Test that we step down if the term is greater than our current term
	node := testNode(t)
	node.state.currentTerm = 1
	node.role = Candidate
	req := AppendEntriesRequest{
		Term:         2,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	resp := node.checkAppendEntriesRequest(req)
	if !resp.Success {
		t.Errorf("Expected entries to be accepted")
	}
	if node.role != Follower {
		t.Errorf("Expected node to be a follower")
	}
	if node.state.currentTerm != 2 {
		t.Errorf("Expected term to be updated")
	}
	if node.state.votedFor != 2 {
		t.Errorf("Expected voted for to be updated")
	}
}

func TestCheckAppendEntriesPrevLogIndexNotFound(t *testing.T) {
	// Test that we refuse the entries if the prevLogIndex is not found
	// ie, we're missing a log entry
	node := testNode(t)
	node.state.currentTerm = 1
	node.role = Follower
	nodeEntries := []LogEntry{
		{
			Term:    1,
			Index:   1,
			Type:    RAFT_LOG,
			Command: []byte("test"),
		},
		{
			Term:    2,
			Index:   2,
			Type:    USER_LOG,
			Command: []byte("user command"),
		},
	}
	node.state.Append(nodeEntries)

	req := AppendEntriesRequest{
		Term:         2,
		LeaderId:     2,
		PrevLogIndex: 3,
		PrevLogTerm:  2,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	resp := node.checkAppendEntriesRequest(req)
	if resp.Success {
		t.Errorf("Expected entries to be refused")
	}
}

func TestCheckAppendEntriesPrevLogIndexTermDifferent(t *testing.T) {
	// Test that we refuse the entries if the prevLogIndex is not found
	// ie, we're missing a log entry
	node := testNode(t)
	node.state.currentTerm = 1
	node.role = Follower
	nodeEntries := []LogEntry{
		{
			Term:    1,
			Index:   1,
			Type:    RAFT_LOG,
			Command: []byte("test"),
		},
		{
			Term:    2,
			Index:   2,
			Type:    USER_LOG,
			Command: []byte("user command"),
		},
	}
	node.state.Append(nodeEntries)

	req := AppendEntriesRequest{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  3,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	resp := node.checkAppendEntriesRequest(req)
	if resp.Success {
		lg, _ := node.state.Get(2)
		fmt.Println(lg)
		t.Errorf("Expected entries to be refused")
	}
}
