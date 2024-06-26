package raft

import (
	"log"
)

type ClientRequest struct {
	Command []byte
}

type ClientRequestResponse struct {
	Success bool
}

func (n *Node) clientRequestHandler(request ClientRequest) ClientRequestResponse {
	/* Append the command to the log */
	n.mtx.Lock()
	defer n.mtx.Unlock()
	log.Printf("Node %d received client request %s\n", n.state.id, string(request.Command))
	if n.role != Leader {
		peer, err := n.GetLeader()
		if err != nil {
			log.Printf("Node %d: No leader found\n", n.state.id)
			return ClientRequestResponse{Success: false}
		}
		log.Printf("Node %d: Forwarding request to leader %d\n", n.state.id, peer.Id)
		resp, err := n.ForwardToLeaderRPC(peer, request)
		if err != nil {
			return ClientRequestResponse{Success: false}
		}
		return resp
	} else {
		log.Printf("Node %d (leader): Appending command to log\n", n.state.id)
		n.state.Append([]LogEntry{LogEntry{
			Term:    n.state.currentTerm,
			Index:   n.state.LastLogIndex() + 1,
			Command: request.Command,
		}})
		ok := n.appendEntries()
		return ClientRequestResponse{Success: ok}
	}
}
