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
	n.appendEntries()
	n.RestartHeartbeatTimerLeader()
}

func (n *Node) appendEntries() bool {
	/* Replicate log entries to all peers */
	n.StopTimer()
	replicated := make(chan bool, len(n.Peers))
	for _, peer := range n.Peers {
		if peer.Id == n.state.id {
			continue
		}
		go n.appendEntriesToPeer(peer, replicated)
	}
	replicated_count := 1
	log.Println("leader waiting for replicas")
	for _, peer := range n.Peers {
		if peer.Id == n.state.id {
			continue
		}
		ok := <-replicated
		if ok {
			replicated_count += 1
		}
	}
	n.RestartHeartbeatTimerLeader()
	if replicated_count > len(n.Peers)/2 {
		// More than half of the peers have replicated the log entries
		// If the last log is of our term, commit it and by extension all previous logs
		last_log_term := n.state.LastLogTerm()
		if last_log_term == n.state.currentTerm {
			n.commitEntries()
		}
		log.Println("Node ", n.state.id, " replicated log entries to majority of peers")
		return true
	} else {
		log.Println("Node ", n.state.id, " couldn't replicate log entries to majority of peers")
		return false
	}
}

func (n *Node) getAppendEntryRequest(peer Peer) AppendEntriesRequest {
	/* Get an AppendEntriesRequest for a peer */

	prevLog, err := n.state.Get(n.leaderReplicationState[peer.Id].nextIndex - 1)
	if err != nil {
		// Handle the error
		log.Printf("Error getting prev log for peer %d %d\n", peer.Id, n.leaderReplicationState[peer.Id].nextIndex)
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
	missing_logs_count := n.state.Logger.LastLogIndex() - peerState.nextIndex
	log.Printf("Node %d: peer %d next index %d n.lastlogindex %d\n", n.state.id, peer.Id, peerState.nextIndex, n.state.Logger.LastLogIndex())
	missing_logs, err := n.state.GetRange(peerState.nextIndex, n.state.Logger.LastLogIndex())
	if err != nil {
		// Handle the error
		log.Printf("Error getting missing logs n.LastLogIndex %d peerNextindex %d missing %d\n",
			n.state.Logger.LastLogIndex(),
			peerState.nextIndex,
			missing_logs_count,
		)
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

func (n *Node) appendEntriesToPeer(peer Peer, replicated chan bool) {
	/* Append entries to a peer */
	n.Lock()
	request := n.getAppendEntryRequest(peer)
	n.Unlock()
	response, err := n.RaftRPC.AppendEntriesRPC(peer, request)
	n.Lock()
	defer n.Unlock()
	if err == nil {
		state := n.leaderReplicationState[peer.Id]
		if response.Success {
			replicated <- true
			if len(request.Entries) == 0 {
				// heartbeat
				return
			}
			last_replicated_log_index := request.Entries[len(request.Entries)-1].Index
			state.nextIndex = last_replicated_log_index + 1
			state.matchIndex = last_replicated_log_index
			n.leaderReplicationState[peer.Id] = state
		} else {
			// handle term difference later
			if state.nextIndex > 0 {
				state.nextIndex -= 1
				log.Println("Node ", n.state.id, " decremented next index for peer ", peer.Id, " to ", state.nextIndex)
			} else {
				log.Panic("Node ", n.state.id, " can't find a common log entry with peer ", peer.Id)
			}
			n.leaderReplicationState[peer.Id] = state
			replicated <- false
		}
	} else {
		replicated <- false
		log.Printf("Node %d: network error sending entries to peer %d: %s\n", n.state.id, peer.Id, err)
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

func (n *Node) write(request ClientRequest) {
	/* Write entries to the log */
	logEntry := LogEntry{
		Term:    n.state.currentTerm,
		Index:   n.state.Logger.LastLogIndex() + 1,
		Type:    USER_LOG,
		Command: request.Command,
	}

	n.state.Logger.Append([]LogEntry{logEntry})
	replicated := n.appendEntries()
	n.channels.clientResponseChannel <- ClientRequestResponse{Success: replicated}
}
