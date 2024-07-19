package raft

import (
	"log"
)

func (n *Node) recvAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
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
	if err != nil || lg.Term != req.PrevLogTerm {
		log.Println("AppendEntries: Log doesn't match", err, lg.Term, req.PrevLogTerm)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: false}
	}

	n.role = Follower
	n.state.votedFor = req.LeaderId
	if len(req.Entries) == 0 {
		n.RestartHeartbeatTimer()
		log.Printf("Node %d: AppendEntries: Heartbeat from leader %d\n", n.state.id, req.LeaderId)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: true}
	}

	// Append new entries
	n.state.TruncateTo(req.PrevLogIndex + 1)
	n.state.Append(req.Entries)
	last_appended_index := req.Entries[len(req.Entries)-1].Index
	log.Printf("Node %d: AppendEntries: Appending %d new entries, last appended index= %d\n", n.state.id, len(req.Entries), last_appended_index)

	return AppendEntriesResponse{n.state.currentTerm, true}
}
