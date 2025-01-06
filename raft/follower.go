package raft

import (
	"log"
)

func (n *Node) checkAppendEntriesRequest(req AppendEntriesRequest) AppendEntriesResponse {
	/* AppendEntries RPC */
	if req.Term < n.state.currentTerm {
		log.Printf("Node %d: AppendEntries: Term %d < currentTerm %d\n", n.state.id, req.Term, n.state.currentTerm)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: false}
	}
	if n.state.currentTerm <= req.Term && n.role == Candidate {
		log.Printf("Node %d: AppendEntries: Term %d >= currentTerm %d, stepping down\n", n.state.id, req.Term, n.state.currentTerm)
		n.role = Follower
		n.state.currentTerm = req.Term
		n.state.votedFor = req.LeaderId
		n.RestartHeartbeatTimer()
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: true}
	}

	n.RestartHeartbeatTimer()
	lg, err := n.state.Get(req.PrevLogIndex)
	if err != nil {
		log.Println("AppendEntries: coudn't get log at index", req.PrevLogIndex)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: false}
	}
	if lg.Term != req.PrevLogTerm {
		log.Printf("AppendEntries: Log doesn't match log at index term: %d req prevlogterm %d\n", lg.Term, req.PrevLogTerm)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: false}
	}
	if n.role != Follower {
		n.loseElection()
	}
	n.role = Follower
	n.state.votedFor = req.LeaderId
	if len(req.Entries) == 0 {
		log.Printf("Node %d: AppendEntries: Heartbeat from leader %d\n", n.state.id, req.LeaderId)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: true}
	}

	// Append new entries
	n.state.TruncateTo(req.PrevLogIndex)
	n.state.Append(req.Entries)
	last_appended_index := req.Entries[len(req.Entries)-1].Index
	log.Printf("Node %d: AppendEntries: Appending %d new entries, last appended index= %d got %v\n",
		n.state.id,
		len(req.Entries),
		last_appended_index,
		req.Entries,
	)
	if n.state.commitIndex < req.LeaderCommit {
		n.commitEntries()
	}
	return AppendEntriesResponse{n.state.currentTerm, true}
}

func (n *Node) handleRecvAppendEntries(req AppendEntriesRequest) {
	/* Called by a node when it receives an AppendEntries request */
	resp := n.checkAppendEntriesRequest(req)
	n.channels.appendEntriesResponseChannel <- resp
}

func (n *Node) RecvAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	/* Called by the RPC layer when a node receives an AppendEntries RPC */
	n.channels.appendEntriesRequestChannel <- req
	return <-n.channels.appendEntriesResponseChannel
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderId          uint64
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	LastConfig        []Peer
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term    uint64
	Success bool
}

func (n *Node) InstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	/*
		InstallSnapshot RPC
		Todo: implement chunking
	*/
	if req.Term < n.state.currentTerm {
		log.Printf("Node %d: InstallSnapshot: Term %d < currentTerm %d\n", n.state.id, req.Term, n.state.currentTerm)
		return InstallSnapshotResponse{Term: n.state.currentTerm, Success: false}
	}
	// Todo: deserialize snapshot etc..
	n.state.lastApplied = req.LastIncludedIndex
	n.Peers = req.LastConfig
	n.RestartHeartbeatTimer()
	return InstallSnapshotResponse{Term: n.state.currentTerm, Success: true}
}

func (n *Node) handleInstallSnapshotRequest(req InstallSnapshotRequest) {
	/* Called by a node when it receives an InstallSnapshot request */
	resp := n.InstallSnapshot(req)
	n.channels.installSnapshotResponseChannel <- resp
}

func (n *Node) RecvInstallSnapshotRequest(req InstallSnapshotRequest) InstallSnapshotResponse {
	/* Called by the RPC layer when a node receives an InstallSnapshot RPC */
	n.channels.installSnapshotRequestChannel <- req
	return <-n.channels.installSnapshotResponseChannel
}
