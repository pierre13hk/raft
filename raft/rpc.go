package raft

//"fmt"

type Peer struct {
	Id   uint64
	Addr string
}

type RaftRPC interface {
	// Register a node with the RPC layer
	RegisterNode(node *Node)
	// Start the RPC server
	Start()
	// Send a request to a peer to vote for us
	RequestVoteRPC(peer Peer, ballot Ballot) (BallotResponse, error)
	// Ask a peer to append entries to their log
	AppendEntriesRPC(peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error)
	// Forward a client request to the leader
	ForwardToLeaderRPC(peer Peer, req ClientRequest) (ClientRequestResponse, error)
	// Send a join cluster request to a peer
	JoinClusterRPC(peer Peer, req JoinClusterRequest) (JoinClusterResponse, error)
	// Send a snapshot to a peer
	//InstallSnapshotRPC(peer Peer, req InstallSnapshotRequest) (InstallSnapshotResponse, error)
	// --- Client RPCs ---
	// Add a client to the cluster
	AddClientRPC() (*ClusterInfo, error)
	// Hanlde a client write request
	ClientWriteRPC(peer Peer, req ClientRequest) (ClientRequestResponse, error)
}
