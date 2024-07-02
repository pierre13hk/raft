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

func (n *Node) leaderHeartbeat() {
	/* Send AppendEntries RPC to all peers */
	/*
		request := AppendEntriesRequest{
			Term:         n.state.currentTerm,
			LeaderId:     n.state.id,
			PrevLogIndex: n.state.LastLogIndex(),
			PrevLogTerm:  n.state.LastLogTerm(),
			Entries:      []LogEntry{},
			LeaderCommit: n.state.commitIndex,
		}
	*/
	channel := make(chan bool, len(n.Peers))
	for _, peer := range n.Peers {
		if peer.Id == n.state.id {
			continue
		}
		go func(p Peer) {
			n.appendEntriesToPeer(p, channel)
		}(peer)
	}
	n.RestartHeartbeatTimerLeader()
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
	n.leaderHeartbeat()
	n.leaderReplicationState = make(map[uint64]FollowerReplicationState)
	for _, peer := range n.Peers {
		n.leaderReplicationState[peer.Id] = FollowerReplicationState{
			nextIndex:  n.state.LastLogIndex() + 1,
			matchIndex: 0,
		}
	}
	for n.role == Leader {
		n.leaderHeartbeat()
		time.Sleep(500 * time.Millisecond)
	}
}

func (n *Node) appendEntries() bool {
	/* Replicate log entries to all peers */
	responseChannel := make(chan bool, len(n.Peers))
	for _, peer := range n.Peers {
		if peer.Id == n.state.id {
			continue
		}
		if n.leaderReplicationState[peer.Id].nextIndex <= n.state.LastLogIndex() {
			go n.appendEntriesToPeer(peer, responseChannel)
		}
	}
	log.Printf("Node %d : Waiting for responses\n", n.state.id)
	replicatedCount := 0
	for range n.Peers {
		<-responseChannel
		replicatedCount += 1
	}
	log.Printf("Node %d : Got all responses\n", n.state.id)
	if replicatedCount > len(n.Peers)/2 {
		/* Majority replicated */
		n.state.commitIndex = n.largestCommittedIndex(&n.leaderReplicationState)
		return true
	}
	return false
}

func (n *Node) appendEntriesToPeer(peer Peer, responseChannel chan bool) {
	/* Replicate log entries to a peer */
	peerState := n.leaderReplicationState[peer.Id]
	entries, _ := n.state.GetRange(peerState.nextIndex)
	if entries == nil {
		responseChannel <- true
		return
	}
	previousLog, _ := n.state.Get(peerState.nextIndex - 1)
	request := AppendEntriesRequest{
		Term:         n.state.currentTerm,
		LeaderId:     n.state.id,
		PrevLogIndex: previousLog.Index,
		PrevLogTerm:  previousLog.Term,
		Entries:      entries,
		LeaderCommit: n.state.commitIndex,
	}
	response, _ := n.AppendEntriesRPC(peer, request)
	if response.Success {
		peerState.nextIndex += uint64(len(entries))
		peerState.matchIndex = peerState.nextIndex - 1
		n.leaderReplicationState[peer.Id] = peerState
		responseChannel <- true
	} else {
		peerState.nextIndex -= 1
		n.leaderReplicationState[peer.Id] = peerState
		responseChannel <- false
	}
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
