package raft

import (
	"fmt"
	"testing"
)

func TestInitialHeartBeat(t *testing.T) {
	/*
		Test that the leader sends a heartbeat to all peers when it becomes the leader.
	*/
	node := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	node.role = Leader
	node.leaderReplicationState = make(map[uint64]FollowerReplicationState)
	node.leaderReplicationState[2] = FollowerReplicationState{nextIndex: 1, matchIndex: 0}
	heartbeatRequest := node.getAppendEntryRequest(Peer{Id: 2})
	if len(heartbeatRequest.Entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(heartbeatRequest.Entries))
	}
	if heartbeatRequest.LeaderCommit != 0 {
		t.Fatalf("expected leader commit to be 0, got %d", heartbeatRequest.LeaderCommit)
	}
	follower := NewNode(2, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	response := follower.checkAppendEntriesRequest(heartbeatRequest)
	if !response.Success {
		t.Fatalf("expected success, got %v", response)
	}
}
func TestLargestCommitedIndex(t *testing.T) {
	replicationState := make(map[uint64]FollowerReplicationState)
	replicationState[1] = FollowerReplicationState{nextIndex: 1, matchIndex: 0}
	replicationState[2] = FollowerReplicationState{nextIndex: 1, matchIndex: 1}
	replicationState[3] = FollowerReplicationState{nextIndex: 1, matchIndex: 2}
	replicationState[4] = FollowerReplicationState{nextIndex: 1, matchIndex: 3}
	replicationState[5] = FollowerReplicationState{nextIndex: 1, matchIndex: 4}

	node := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	node.role = Leader
	node.leaderReplicationState = replicationState

	commitIndex := node.largestCommittedIndex(&replicationState)
	if commitIndex != 2 {
		t.Fatalf("expected commit index to be 2, got %d", commitIndex)
	}

	delete(replicationState, 5)
	commitIndex = node.largestCommittedIndex(&replicationState)
	if commitIndex != 1 {
		t.Fatalf("expected commit index to be 1, got %d", commitIndex)
	}
}

func TestAppendEntriesNewLog(t *testing.T) {
	/*
		Test the case where the leader sends an append entries request to a peer
		whose log is behind.
	*/
	replicationState := make(map[uint64]FollowerReplicationState)
	replicationState[1] = FollowerReplicationState{nextIndex: 1, matchIndex: 0}
	replicationState[2] = FollowerReplicationState{nextIndex: 1, matchIndex: 0}

	leader := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	leader.state.Append([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("first log")},
	})
	leader.role = Leader
	leader.leaderReplicationState = replicationState
	leader.state.commitIndex = leader.largestCommittedIndex(&replicationState)

	request := leader.getAppendEntryRequest(Peer{Id: 2})
	if len(request.Entries) != 1 {
		fmt.Println(request.Entries)
		t.Fatalf("expected 1 entry, got %d", len(request.Entries))
	}
	if request.Entries[0].Index != 1 {
		t.Fatalf("expected first entry to be index 1, got %d", request.Entries[0].Index)
	}
	if request.LeaderCommit != leader.state.commitIndex {
		t.Fatalf("expected leader commit to be %d, got %d", leader.state.commitIndex, request.LeaderCommit)
	}
	if string(request.Entries[0].Command) != "first log" {
		t.Fatalf("expected command to be 'first log', got %s", request.Entries[0].Command)
	}
}

func TestAppendEntriesToPeerLate(t *testing.T) {
	/*
		Test the case where the leader sends an append entries request to a peer
		whose log is behind.
	*/
	replicationState := make(map[uint64]FollowerReplicationState)
	replicationState[1] = FollowerReplicationState{nextIndex: 1, matchIndex: 4}
	replicationState[2] = FollowerReplicationState{nextIndex: 1, matchIndex: 0}
	replicationState[3] = FollowerReplicationState{nextIndex: 1, matchIndex: 2}
	replicationState[4] = FollowerReplicationState{nextIndex: 1, matchIndex: 3}
	replicationState[5] = FollowerReplicationState{nextIndex: 1, matchIndex: 4}

	leader := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	leader.state.Append([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("first log")},
		{Term: 1, Index: 2, Command: []byte("second log")},
		{Term: 1, Index: 3, Command: []byte("third log")},
	})
	leader.role = Leader
	leader.leaderReplicationState = replicationState
	leader.state.commitIndex = leader.largestCommittedIndex(&replicationState)

	request := leader.getAppendEntryRequest(Peer{Id: 2})
	if len(request.Entries) != 3 {
		fmt.Println(request.Entries)
		t.Fatalf("expected 3 entries, got %d", len(request.Entries))
	}
	if request.Entries[0].Index != 1 {
		t.Fatalf("expected first entry to be index 1, got %d", request.Entries[0].Index)
	}
	if request.LeaderCommit != leader.state.commitIndex {
		t.Fatalf("expected leader commit to be %d, got %d", leader.state.commitIndex, request.LeaderCommit)
	}
}

func TestAppendEntriesToPeerOnTime(t *testing.T) {
	/*
		Test the case where the leader sends an append entries request to a peer
		whose log is at the same index. It should send a heartbeat.
	*/
	replicationState := make(map[uint64]FollowerReplicationState)
	replicationState[1] = FollowerReplicationState{nextIndex: 1, matchIndex: 4}
	replicationState[2] = FollowerReplicationState{nextIndex: 4, matchIndex: 0}
	replicationState[3] = FollowerReplicationState{nextIndex: 1, matchIndex: 2}
	replicationState[4] = FollowerReplicationState{nextIndex: 1, matchIndex: 3}
	replicationState[5] = FollowerReplicationState{nextIndex: 1, matchIndex: 4}

	leader := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	leader.role = Leader
	leader.leaderReplicationState = replicationState
	leader.state.commitIndex = leader.largestCommittedIndex(&replicationState)

	request := leader.getAppendEntryRequest(Peer{Id: 2})
	if len(request.Entries) != 0 {
		fmt.Println(request.Entries)
		t.Fatalf("expected 0 entries, got %d", len(request.Entries))
	}

	if request.LeaderCommit != leader.state.commitIndex {
		t.Fatalf("expected leader commit to be %d, got %d", leader.state.commitIndex, request.LeaderCommit)
	}
}

func TestCheckJoinClusterRequestAddingPeer(t *testing.T) {
	/*
		Test that we can't add a peer if we're already adding a peer.
	*/
	node := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	node.role = Follower | AddingPeer
	request := JoinClusterRequest{Id: 4, Addr: "localhost:1234", Port: "1234"}
	canAddErr := node.checkCanAddPeer(request)
	if canAddErr == nil {
		t.Fatalf("expected err, got")
	}
}

func TestCheckJoinClusterRequestNotLeader(t *testing.T) {
	/*
		Test that we can't add a peer if we're not the leader.
	*/
	node := NewNode(1, "localhost:1234", NewInMemoryRaftRPC(), &DebugStateMachine{}, t.TempDir())
	node.role = Follower
	request := JoinClusterRequest{Id: 4, Addr: "localhost:1234", Port: "1234"}
	canAddErr := node.checkCanAddPeer(request)
	if canAddErr == nil {
		t.Fatalf("expected err, got")
	}
}
