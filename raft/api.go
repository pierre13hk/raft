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

func (n *Node) RecvClientRequest(request ClientRequest) ClientRequestResponse {
	// Only handle one client request at a time
	n.clientRequestMutex.Lock()
	defer n.clientRequestMutex.Unlock()

	if n.role != Leader {
		log.Println("RecvClientRequest: Node ", n.state.id, " is not the leader")
		return ClientRequestResponse{Success: false}
	}

	n.channels.clientRequestChannel <- request
	response := <-n.channels.clientResponseChannel
	if !response.Success {
		log.Println("RecvClientRequest: Node ", n.state.id, " failed to handle client request")
	}
	log.Println("RecvClientRequest: Node ", n.state.id, " handled client request")
	return ClientRequestResponse{Success: response.Success}

}

type JoinClusterRequest struct {
	Id   uint64
	Addr string
	Port string
}

type JoinClusterResponse struct {
	Success bool
	Message string
}

func (n *Node) BootstrapCluster(addr string, port string, peersAddrs ...string) bool {
	joinRequest := JoinClusterRequest{
		Id:   n.state.id,
		Addr: addr,
		Port: port,
	}
	for _, peer := range peersAddrs {
		peer := Peer{Id: 0, Addr: peer}
		joinResponse, err := n.JoinClusterRPC(peer, joinRequest)
		if err != nil {
			log.Println("BootstrapCluster: Node ", n.state.id, " failed to join cluster, error: ", err)
		}
		if !joinResponse.Success {
			log.Println("BootstrapCluster: Node ", n.state.id, " failed to join cluster: ", joinResponse.Message)
			continue
		}
		log.Println("BootstrapCluster: Node ", n.state.id, " joined cluster")
		n.Start()
		return true
	}
	return false
}
