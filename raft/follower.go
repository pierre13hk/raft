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
		close(n.electionChannel)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: true}
	}

	n.RestartHeartbeatTimer()
	lg, err := n.state.Get(req.PrevLogIndex)
	if err != nil || lg.Term != req.PrevLogTerm {
		log.Println("AppendEntries: Log doesn't match", err, lg.Term, req.PrevLogTerm)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: false}
	}

	if n.role == Candidate {
		close(n.electionChannel)
	}
	n.role = Follower
	n.state.votedFor = req.LeaderId
	if len(req.Entries) == 0 {
		// heartbeat
		n.timerBackoffCounter = 1
		log.Printf("Node %d: AppendEntries: Heartbeat from leader %d\n", n.state.id, req.LeaderId)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: true}
	}

	//logs, _ := n.state.GetRange(req.PrevLogIndex + 1)
	if req.Entries[0].Index <= n.state.LastLogIndex()-1 {
		for _, entry := range req.Entries {
			if entry.Index > n.state.LastLogIndex() {
				break
			}
			localLog, err := n.state.Get(entry.Index)
			if err != nil {
				log.Println("shouldnt happen")
				break
			}
			// A node can't have a more up to date log than the leader
			// So req.Entries[i] should always be valid / in bounds
			if entry.Term != localLog.Term {
				log.Println("Node % d AppendEntries: Log doesn't match, truncating from ", n.state.id, entry.Index, entry.Command)
				n.state.TruncateTo(entry.Index)
				break
			}
		}
	}

	new := 0
	for i, entry := range req.Entries {
		_, err := n.state.Get(entry.Index)
		if err != nil {
			new = i
			break
		}
	}
	log.Printf("Node %d: AppendEntries: Appending %d new entries new = %d, len reqent %d\n", n.state.id, len(req.Entries[new:]), new, len(req.Entries))
	n.state.Append(req.Entries[new:])

	return AppendEntriesResponse{n.state.currentTerm, true}
}
