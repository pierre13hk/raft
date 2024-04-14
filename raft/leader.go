package raft

import (
	"log"
	"slices"
	"strconv"
	"time"
)

type FollowerReplicationState struct {
	nextIndex  uint64
	matchIndex uint64
}

func (n *Node) leaderHeartbeat() {
	/* Send AppendEntries RPC to all peers */
	request := AppendEntriesRequest{
		Term:         n.state.currentTerm,
		LeaderId:     n.state.id,
		PrevLogIndex: n.state.logger.LastLogIndex(),
		PrevLogTerm:  n.state.logger.LastLogTerm(),
		Entries:      []LogEntry{},
		LeaderCommit: n.state.commitIndex,
	}

	for _, peer := range n.Peers {
		if peer.Id == n.state.id {
			continue
		}
		go func(p Peer) {
			n.RaftRPC.AppendEntriesRPC(p, request)
		}(peer)
	}

	n.RestartHeartbeatTimerLeader()
}

func (n *Node) RestartHeartbeatTimerLeader() {
	/* Restart the heartbeat timer */
	n.timer.Stop()
	n.timer.Reset(time.Duration(500) * time.Millisecond)
}

func (n *Node) leaderDaemon() {
	// clear the map and initialize it.
	n.leaderHeartbeat()
	n.leaderReplicationState = make(map[uint64]FollowerReplicationState)
	for _, peer := range n.Peers {
		n.leaderReplicationState[peer.Id] = FollowerReplicationState{
			nextIndex:  n.state.logger.LastLogIndex() + 1,
			matchIndex: 0,
		}
	}

	// daemon loop
	msg := "Node " + strconv.FormatUint(n.state.id, 10) + " : saying hello!"
	entry := LogEntry{
		Term:    n.state.currentTerm,
		Index:   n.state.logger.LastLogIndex() + 1,
		Command: []byte(msg),
	}
	n.state.logger.Append([]LogEntry{entry})
	n.appendEntries()
	n.timer.Stop()
	for i := 0; i < 10; i++ {
		time.Sleep(200 * time.Millisecond)
	}

}

func (n *Node) appendEntries() {
	/* Replicate log entries to all peers */
	for _, peer := range n.Peers {
		if peer.Id == n.state.id {
			continue
		}
		if n.leaderReplicationState[peer.Id].nextIndex <= n.state.logger.LastLogIndex() {
			n.appendEntriesToPeer(peer)
		}
	}
}

func (n *Node) appendEntriesToPeer(peer Peer) {
	/* Replicate log entries to a peer */
	peerUpToDate := false
	for !peerUpToDate {
		state := n.leaderReplicationState[peer.Id]
		prevLog, _ := n.state.logger.Get(state.nextIndex - 1)
		entries, _ := n.state.logger.GetRange(state.nextIndex)
		request := AppendEntriesRequest{
			Term:         n.state.currentTerm,
			LeaderId:     n.state.id,
			PrevLogIndex: state.nextIndex - 1,
			PrevLogTerm:  prevLog.Term,
			Entries:      entries,
			LeaderCommit: n.state.commitIndex,
		}

		response, error := n.RaftRPC.AppendEntriesRPC(peer, request)
		peerUpToDate = true
		log.Printf("Node %d : AppendEntries got resp\n", n.state.id)
		if error != nil {
			if response.Success {
				state.nextIndex = n.state.logger.LastLogIndex()
				state.matchIndex = state.nextIndex
				n.leaderReplicationState[peer.Id] = state
				peerUpToDate = true
			} else {
				log.Printf("Node %d : AppendEntries failed\n", n.state.id)
				state.nextIndex -= 1
			}

		} else {
			// retry? exponential backoff?
		}
	}
}

func (n *Node) largestCommitedIndex(p *map[uint64]FollowerReplicationState) uint64 {
	/* Return the largest commit index */
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
