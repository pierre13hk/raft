package raft

import (
	"log"
	"slices"
	"time"
)

type FollowerReplicationState struct {
	nextIndex  uint64
	matchIndex uint64
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderId     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

func (n *Node) RestartHeartbeatTimerLeader() {
	/* Restart the heartbeat timer */
	n.timer.Stop()
	n.timer.Reset(time.Duration(500) * time.Millisecond)
}

func (n *Node) becomeLeader() {
	/* Become the leader */
	n.role = Leader
	n.leaderDaemon()
}

func (n *Node) leaderDaemon() {
	// clear the map and initialize it.
	n.leaderReplicationState = make(map[uint64]FollowerReplicationState)
	for _, peer := range n.Peers {
		n.leaderReplicationState[peer.Id] = FollowerReplicationState{
			nextIndex:  n.state.LastLogIndex() + 1,
			matchIndex: 0,
		}
	}
	for n.role == Leader {
		time.Sleep(500 * time.Millisecond)
		n.appendEntries()
	}
}

func (n *Node) appendEntries() {
	/* Replicate log entries to all peers */

	for _, peer := range n.Peers {
		if peer.Id == n.state.id {
			continue
		}
		go n.appendEntriesToPeer(peer)
	}
}

func (n *Node) getAppendEntryRequest(peer Peer) AppendEntriesRequest {
	/* Get an AppendEntriesRequest for a peer */
	prevLog, err := n.state.Get(n.leaderReplicationState[peer.Id].nextIndex - 1)
	if err != nil {
		// Handle the error
	}

	peerState := n.leaderReplicationState[peer.Id]
	if peerState.nextIndex > n.state.Logger.LastLogIndex() {
		// No new entries to replicate, send heartbeat
		return AppendEntriesRequest{
			Term:         n.state.currentTerm,
			LeaderId:     n.state.id,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      []LogEntry{},
			LeaderCommit: n.state.commitIndex,
		}
	}
	missing_logs_count := n.state.Logger.LastLogIndex() - prevLog.Index
	log.Println("Missing logs count", missing_logs_count)
	if missing_logs_count > 100 {
		missing_logs_count = 100
	}
	missing_logs, err := n.state.GetRange(peerState.nextIndex, peerState.nextIndex+missing_logs_count)
	if err != nil {
		// Handle the error
	}
	request := AppendEntriesRequest{
		Term:         n.state.currentTerm,
		LeaderId:     n.state.id,
		PrevLogIndex: prevLog.Index,
		PrevLogTerm:  prevLog.Term,
		Entries:      missing_logs,
		LeaderCommit: n.state.commitIndex,
	}
	return request
}

func (n *Node) appendEntriesToPeer(peer Peer) {
	/* Append entries to a peer */
	request := n.getAppendEntryRequest(peer)
	response, err := n.RaftRPC.AppendEntriesRPC(peer, request)
	if err == nil {
		n.channels.appendEntriesResponseChannel <- response
	} else {
		// Handle the error
	}

}

func (n *Node) handleAppendEntriesResponse(response AppendEntriesResponse) {
	// Handle a response from peer a who me made an AppendEntries RPC to
}

func (n *Node) sendHeartbeat(peer Peer) {
}

func (n *Node) largestCommittedIndex(p *map[uint64]FollowerReplicationState) uint64 {
	/* Return the largest committed index */
	matchIndexes := make([]uint64, len(*p))
	i := 0
	for _, peer := range *p {
		matchIndexes[i] = peer.matchIndex
		i += 1
	}
	slices.SortFunc(matchIndexes, func(a, b uint64) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	idx := len(matchIndexes) / 2
	if len(matchIndexes)%2 == 0 {
		idx -= 1
	}
	return matchIndexes[idx]
}
