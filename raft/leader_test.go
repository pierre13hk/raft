package raft

import (
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
