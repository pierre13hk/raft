package raft

import "log"

type ClusterInfo struct {
	LeaderId      uint64
	LeaderAddress string
	Peers         []Peer
}

func (n *Node) HandleClientHello() ClusterInfo {
	return ClusterInfo{
		LeaderId:      n.state.votedFor,
		LeaderAddress: n.getPeer(n.state.votedFor).Addr,
		Peers:         n.Peers,
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
	} else {
		log.Println("RecvClientRequest: Node ", n.state.id, " handled client request")
	}
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
	n.StartRPCServer()
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

type ClientReadRequest struct {
	Command []byte
}

type ClientReadResponse struct {
	Response []byte
	Success  bool
	Error    string
}

func (n *Node) handleClientReadRequest(request ClientReadRequest) {
	if n.role != Leader {
		response := ClientReadResponse{
			Response: nil,
			Success:  false,
			Error:    "Node is not the leader",
		}
		n.channels.clientReadResponseChannel <- response
		return
	}
	replicated := n.appendEntries()
	if !replicated {
		response := ClientReadResponse{
			Response: nil,
			Success:  false,
			Error:    "Failed to replicate log",
		}
		n.channels.clientReadResponseChannel <- response
		return
	}
	// The state machine will only read from commited entries
	readBytes, err := n.StateMachine.Read(request.Command)
	if err != nil {
		response := ClientReadResponse{
			Response: nil,
			Success:  false,
			Error:    err.Error(),
		}
		n.channels.clientReadResponseChannel <- response
	}
	response := ClientReadResponse{
		Response: readBytes,
		Success:  true,
		Error:    "",
	}
	n.channels.clientReadResponseChannel <- response
}

func (n *Node) RecvClientReadRequest(request ClientReadRequest) ClientReadResponse {
	n.channels.clientReadRequestChannel <- request
	return <-n.channels.clientReadResponseChannel
}
