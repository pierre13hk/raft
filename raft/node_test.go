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

	resp := node.checkAppendEntriesRequest(req1)
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

	resp := node.checkAppendEntriesRequest(req1)
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

	resp = node.checkAppendEntriesRequest(req2)
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

	resp := node.checkAppendEntriesRequest(req)
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

	resp := node.checkAppendEntriesRequest(req)
	if !resp.Success {
		t.Fatalf("Expected success")
	}

	log, _ := node.state.Get(3)
	if log.Term != 3 || log.Command[0] != 'x' {
		t.Errorf("Wrong term %d %s", log.Term, string(log.Command[0]))
	}

}

func TestAddPeerWrongLogType(t *testing.T) {
	/*
		Test the addPeer method with a log entry that is not a cluster change
	*/
	node := NewNode(10, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	err := node.addPeer(LogEntry{Type: USER_LOG, Command: []byte("1,localhost,1234")})
	if err == nil {
		t.Errorf("Expected error")
	}
	if len(node.Peers) != 0 {
		t.Errorf("Expected 0 peers")
	}
}

func TestAddPeerMissingInfo(t *testing.T) {
	/*
		Test the addPeer method with a log entry that is not a cluster change
	*/
	node := NewNode(10, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	err := node.addPeer(LogEntry{Type: CLUSTER_CHANGE_ADD, Command: []byte("1,")})
	if err == nil {
		t.Errorf("Expected error")
	}
	if len(node.Peers) != 0 {
		t.Errorf("Expected 0 peers")
	}
	node.addPeer(LogEntry{Type: CLUSTER_CHANGE_ADD, Command: []byte("1,,")})
	if len(node.Peers) != 0 {
		t.Errorf("Expected 0 peers")
	}
}

func TestAddPeerMalformedID(t *testing.T) {
	node := NewNode(10, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	err := node.addPeer(LogEntry{Type: CLUSTER_CHANGE_ADD, Command: []byte("abc,localhost,1234")})
	if err == nil {
		t.Errorf("Expected error")
	}
	if len(node.Peers) != 0 {
		t.Errorf("Expected 0 peers")
	}
}

func TestAddPeerMalformedAddr(t *testing.T) {
	node := NewNode(10, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	node.addPeer(LogEntry{Type: CLUSTER_CHANGE_ADD, Command: []byte("1|127.0.0.1:8000|8000")})
	if len(node.Peers) != 0 {
		t.Errorf("Expected 0 peers")
	}
}

func TestAddPeer(t *testing.T) {
	node := NewNode(10, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	err := node.addPeer(LogEntry{Type: CLUSTER_CHANGE_ADD, Command: []byte("1|127.0.0.1|1234")})
	if err != nil {
		t.Errorf("Error adding peer")
	}
	if len(node.Peers) != 1 {
		t.Errorf("Expected 1 peer, was %d", len(node.Peers))
	}
}

func TestDoubleAdd(t *testing.T) {
	node := NewNode(10, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	log := LogEntry{Type: CLUSTER_CHANGE_ADD, Command: []byte("1|127.0.0.1|1234")}
	err := node.addPeer(log)
	if err != nil {
		t.Errorf("Error adding peer")
	}
	if len(node.Peers) != 1 {
		t.Errorf("Expected 1 peer, was %d", len(node.Peers))
	}
	err = node.addPeer(log)
	if err != nil {
		t.Errorf("Didn't expect error")
	}
	if len(node.Peers) != 1 {
		t.Errorf("Expected 1 peer, was %d", len(node.Peers))
	}
}
