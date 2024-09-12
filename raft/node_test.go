package raft

import (
	"testing"
)

func TestAppendEntriesSimple(t *testing.T) {
	/*
		A first election has been held and a leader is sending his first log
	*/
	node := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())

	// The first data append entries request
	// sent bn the leader to the followers
	// The leader's term is 2 and he has not yet committed any data.
	req1 := AppendEntriesRequest{
		Term:         2,
		LeaderId:     123,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 2, Index: 1, Command: []byte("a")},
		},
		LeaderCommit: 0,
	}

	resp := node.RecvAppendEntries(req1)
	if !resp.Success {
		t.Errorf("Expected success")
	}

}

func TestAppendEntriesWithConflict(t *testing.T) {
	/*
		A first election has been held and a leader is sending his first log
	*/
	node := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())

	// The first data append entries request
	// sent by the leader to the followers
	// The leader's term is 2 and he has not yet committed any data.
	req1 := AppendEntriesRequest{
		Term:         2,
		LeaderId:     123,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 2, Index: 1, Command: []byte("a")},
		},
		LeaderCommit: 1,
	}

	resp := node.RecvAppendEntries(req1)
	if !resp.Success {
		t.Errorf("Expected success")
	}

	log, _ := node.state.Get(1)
	if log.Term != 2 || log.Command[0] != 'a' {
		t.Errorf("Wrong term %d", log.Term)
	}

	// The second data append entries request
	// sent by the leader to the followers
	// The leader's term is 2 and he has not yet committed any data.
	req2 := AppendEntriesRequest{
		Term:         2,
		LeaderId:     123,
		PrevLogIndex: 3,
		PrevLogTerm:  2,
		Entries: []LogEntry{
			{Term: 2, Index: 4, Command: []byte("b")},
		},
		LeaderCommit: 1,
	}

	resp = node.RecvAppendEntries(req2)
	if resp.Success {
		t.Errorf("Expected failure")
	}

}

func TestAppendEntriesWithConflict2(t *testing.T) {
	/*
		A node has recoverd with a log that is longer than the leaders
		logs a, b and c should be deleted
	*/

	node := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	req := AppendEntriesRequest{
		Term:         3,
		LeaderId:     123,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 3, Index: 1, Command: []byte("x")},
			{Term: 3, Index: 2, Command: []byte("y")},
			{Term: 3, Index: 3, Command: []byte("z")},
		},
		LeaderCommit: 1,
	}

	resp := node.RecvAppendEntries(req)
	if !resp.Success {
		t.Errorf("Expected success")
	}

	log, _ := node.state.Get(1)
	if log.Term != 3 || log.Command[0] != 'x' {
		t.Errorf("Wrong term %d %s", log.Term, string(log.Command[0]))
	}
}

func TestAppendEntriesFollowerNewLeader(t *testing.T) {
	/*
		A follower node has not noticed that a new leader has been elected.
		Check that this node will accept the new leader's log entries.
	*/

	node := NewNode(10, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	node.state.Append([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	})
	req := AppendEntriesRequest{
		Term:         3,
		LeaderId:     123,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 3, Index: 2, Command: []byte("x")},
		},
		LeaderCommit: 1,
	}

	resp := node.RecvAppendEntries(req)
	if !resp.Success {
		t.Fatalf("Expected success")
	}

	log, _ := node.state.Get(3)
	if log.Term != 3 || log.Command[0] != 'x' {
		t.Errorf("Wrong term %d %s", log.Term, string(log.Command[0]))
	}

}
