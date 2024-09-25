package raft

import "log"

type ClusterInfo struct {
	LeaderId      uint64
	LeaderAddress string
}

func (n *Node) HandleClientHello() ClusterInfo {
	return ClusterInfo{
		LeaderId:      n.state.votedFor,
		LeaderAddress: n.getPeer(n.state.votedFor).Addr,
	}
}

type ClientRequest struct {
	Command []byte
}

type ClientRequestResponse struct {
	Success bool
}

func (n *Node) HandleClientRequest(request ClientRequest) ClientRequestResponse {
	// Only handle one client request at a time
	n.clientRequestMutex.Lock()
	defer n.clientRequestMutex.Unlock()

	if n.role != Leader {
		log.Println("HandleClientRequest: Node ", n.state.id, " is not the leader")
		return ClientRequestResponse{Success: false}
	}

	n.channels.clientRequestChannel <- request
	response := <-n.channels.clientResponseChannel
	if !response.Success {
		log.Println("HandleClientRequest: Node ", n.state.id, " failed to handle client request")
	}
	log.Println("HandleClientRequest: Node ", n.state.id, " handled client request")
	return ClientRequestResponse{Success: response.Success}

}

type JoinClusterRequest struct {
	NodeId uint64
}

type JoinClusterResponse struct {
	Success bool
}
