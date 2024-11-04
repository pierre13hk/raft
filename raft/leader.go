package raft

import (
	"errors"
	"fmt"
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
	n.heartbeatTimer.Stop()
	n.heartbeatTimer.Reset(time.Duration(n.config.HeartbeatTimeout))
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
	n.StopElectionTimer()
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
	log.Println("Node ", n.state.id, " getting append entry request for peer ", peer.Id, " next index ", n.leaderReplicationState[peer.Id].nextIndex)
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
			if n.state.currentTerm < response.Term {
				/*
					Handle term difference.
					This can happen if a peer has been partionned, and has been starting
					elections that have reached no other peer, and have incremented their
					term past the leader's term.
				*/
				log.Println("Node ", n.state.id, " found term difference with peer ", peer.Id, " forcing new election")
				n.forceNewElection()
			} else {
				if state.nextIndex > 0 {
					state.nextIndex -= 1
					log.Println("Node ", n.state.id, " decremented next index for peer ", peer.Id, " to ", state.nextIndex)
				} else {
					log.Panic("Node ", n.state.id, " can't find a common log entry with peer ", peer.Id)
				}
				n.leaderReplicationState[peer.Id] = state
			}
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

func (n *Node) checkCanAddPeer(request JoinClusterRequest) error {
	/* Handle an add peer request */
	if n.role&AddingPeer != 0 {
		log.Printf("Node %d: already adding peer\n", n.state.id)
		return errors.New("Node is already adding a peer")
	}
	if n.role != Leader {
		return errors.New("Node is not the leader")
	}
	return nil
}

func (n *Node) handleJoinClusterRequest(request JoinClusterRequest) {
	err := n.checkCanAddPeer(request)
	if err != nil {
		n.channels.JoinClusterResponseChannel <- JoinClusterResponse{Success: false, Message: err.Error()}
		return
	}
	log.Printf("Node %d: add peer request %d %s\n", n.state.id, request.Id, request.Addr)
	strId := fmt.Sprint(request.Id)
	logCommand := formatRaftLogCommand(strId, request.Addr, request.Port)
	logEntry := LogEntry{
		Term:    n.state.currentTerm,
		Index:   n.state.Logger.LastLogIndex() + 1,
		Type:    CLUSTER_CHANGE_ADD,
		Command: []byte(logCommand),
	}
	if err := n.addPeer(logEntry); err != nil {
		log.Printf("Node %d: error adding peer %d %s\n", n.state.id, request.Id, request.Addr)
		n.channels.JoinClusterResponseChannel <- JoinClusterResponse{Success: false, Message: err.Error()}
		return
	}

	log.Printf("fml %x\n", n.leaderReplicationState)
	n.state.Logger.Append([]LogEntry{logEntry})
	log.Println()
	replicated := n.appendEntries()
	// go n.rpc.InstallSnapshotRPC(peer, snapshot)
	n.channels.JoinClusterResponseChannel <- JoinClusterResponse{Success: replicated}
}

func (n *Node) RecvJoinClusterRequest(request JoinClusterRequest) JoinClusterResponse {
	/* Receive an add peer request */
	n.channels.addPeerChannel <- request
	return <-n.channels.JoinClusterResponseChannel
}
