package raft

import (
	"fmt"
	"testing"
)

func TestLargestCommitedIndex(t *testing.T) {
	replicationState := make(map[uint64]FollowerReplicationState)
	replicationState[1] = FollowerReplicationState{nextIndex: 1, matchIndex: 0}
	replicationState[2] = FollowerReplicationState{nextIndex: 1, matchIndex: 1}
	replicationState[3] = FollowerReplicationState{nextIndex: 1, matchIndex: 2}
	replicationState[4] = FollowerReplicationState{nextIndex: 1, matchIndex: 3}
	replicationState[5] = FollowerReplicationState{nextIndex: 1, matchIndex: 4}

	node := NewNode(1)
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

func TestAppendEntriesToPeerLate(t *testing.T) {
	/*
		Test the case where the leader sends an append entries request to a peer
		whose log is behind.
	*/
	logger := &InMemoryLogger{
		entries: []LogEntry{
			{Term: 1, Index: 0, Command: []byte("a")},
			{Term: 1, Index: 1, Command: []byte("b")},
			{Term: 1, Index: 2, Command: []byte("c")},
			{Term: 1, Index: 3, Command: []byte("d")},
		},
	}
	replicationState := make(map[uint64]FollowerReplicationState)
	replicationState[1] = FollowerReplicationState{nextIndex: 1, matchIndex: 4}
	replicationState[2] = FollowerReplicationState{nextIndex: 1, matchIndex: 0}
	replicationState[3] = FollowerReplicationState{nextIndex: 1, matchIndex: 2}
	replicationState[4] = FollowerReplicationState{nextIndex: 1, matchIndex: 3}
	replicationState[5] = FollowerReplicationState{nextIndex: 1, matchIndex: 4}

	leader := NewNode(1)
	leader.role = Leader
	leader.state.Logger = logger
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
	logger := &InMemoryLogger{
		entries: []LogEntry{
			{Term: 1, Index: 0, Command: []byte("a")},
			{Term: 1, Index: 1, Command: []byte("b")},
			{Term: 1, Index: 2, Command: []byte("c")},
			{Term: 1, Index: 3, Command: []byte("d")},
		},
	}
	replicationState := make(map[uint64]FollowerReplicationState)
	replicationState[1] = FollowerReplicationState{nextIndex: 1, matchIndex: 4}
	replicationState[2] = FollowerReplicationState{nextIndex: 4, matchIndex: 0}
	replicationState[3] = FollowerReplicationState{nextIndex: 1, matchIndex: 2}
	replicationState[4] = FollowerReplicationState{nextIndex: 1, matchIndex: 3}
	replicationState[5] = FollowerReplicationState{nextIndex: 1, matchIndex: 4}

	leader := NewNode(1)
	leader.role = Leader
	leader.state.Logger = logger
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
