package raft

import (
	"testing"
)

func TestAppendEntriesSimple(t *testing.T) {
	/*
		A first election has been held and a leader is sending his first log
	*/
	node := NewNode(1)
	logger := &InMemoryLogger{
		entries: []LogEntry{
			{Term: 1, Index: 1, Command: []byte("init")},
		},
	}
	node.state.logger = logger

	// The first data append entries request
	// sent bn the leader to the followers
	// The leader's term is 2 and he has not yet committed any data.
	req1 := AppendEntriesRequest{
		Term:         2,
		LeaderId:     123,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 2, Index: 5, Command: []byte("a")},
		},
		LeaderCommit: 1,
	}

	resp := node.recvAppendEntries(req1)
	if !resp.Success {
		t.Errorf("Expected success")
	}

}

func TestAppendEntriesWithConflict(t *testing.T) {
	/*
		A first election has been held and a leader is sending his first log
	*/
	node := NewNode(1)
	logger := &InMemoryLogger{
		entries: []LogEntry{
			{Term: 1, Index: 1, Command: []byte("init")},
			{Term: 2, Index: 2, Command: []byte("a")},
		},
	}
	node.state.logger = logger

	// The first data append entries request
	// sent bn the leader to the followers
	// The leader's term is 2 and he has not yet committed any data.
	req1 := AppendEntriesRequest{
		Term:         2,
		LeaderId:     123,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 2, Index: 2, Command: []byte("a")},
		},
		LeaderCommit: 1,
	}

	resp := node.recvAppendEntries(req1)
	if !resp.Success {
		t.Errorf("Expected success")
	}

	log, _ := node.state.logger.Get(2)
	if log.Term != 2 || log.Command[0] != 'a' {
		t.Errorf("Wrong term %d", log.Term)
	}

	// The second data append entries request
	// sent by the leader to the followers
	// The leader's term is 2 and he has not yet committed any data.
	req2 := AppendEntriesRequest{
		Term:         2,
		LeaderId:     123,
		PrevLogIndex: 2,
		PrevLogTerm:  2,
		Entries: []LogEntry{
			{Term: 2, Index: 2, Command: []byte("b")},
		},
		LeaderCommit: 1,
	}

	resp = node.recvAppendEntries(req2)
	if !resp.Success {
		t.Errorf("Expected failure")
	}

}

func TestAppendEntriesWithConflict2(t *testing.T) {
	/*
		A node has recoverd with a log that is longer than the leaders
		logs b and c should be deleted
	*/

	node := NewNode(1)
	logger := &InMemoryLogger{
		entries: []LogEntry{
			{Term: 1, Index: 1, Command: []byte("init")},
			{Term: 2, Index: 2, Command: []byte("a")},
			{Term: 2, Index: 3, Command: []byte("b")},
			{Term: 2, Index: 4, Command: []byte("c")},
		},
	}
	node.state.logger = logger

	req := AppendEntriesRequest{
		Term:         3,
		LeaderId:     123,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 3, Index: 2, Command: []byte("x")},
			{Term: 3, Index: 3, Command: []byte("y")},
			{Term: 3, Index: 4, Command: []byte("z")},
		},
		LeaderCommit: 1,
	}

	resp := node.recvAppendEntries(req)
	if !resp.Success {
		t.Errorf("Expected success")
	}

	log, _ := node.state.logger.Get(2)
	if log.Term != 3 || log.Command[0] != 'x' {
		t.Errorf("Wrong term %d %s", log.Term, string(log.Command[0]))
	}
}

func TestAppendEntriesFollowerNewLeader(t *testing.T) {
	/*
		A follower node has not noticed that a new leader has been elected.
		Check that this node will accept the new leader's log entries.
	*/

	node := NewNode(10)
	logger := &InMemoryLogger{
		entries: []LogEntry{
			{Term: 1, Index: 1, Command: []byte("init")},
			{Term: 2, Index: 2, Command: []byte("a")},
			{Term: 2, Index: 3, Command: []byte("b")},
		},
	}
	node.state.logger = logger

	req := AppendEntriesRequest{
		Term:         3,
		LeaderId:     123,
		PrevLogIndex: 3,
		PrevLogTerm:  2,
		Entries: []LogEntry{
			{Term: 3, Index: 4, Command: []byte("x")},
		},
		LeaderCommit: 1,
	}

	resp := node.recvAppendEntries(req)
	if !resp.Success {
		t.Errorf("Expected success")
	}

	log, _ := node.state.logger.Get(4)
	if log.Term != 3 || log.Command[0] != 'x' {
		t.Errorf("Wrong term %d %s", log.Term, string(log.Command[0]))
	}

}
