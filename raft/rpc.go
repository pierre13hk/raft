package raft

//"fmt"

type Peer struct {
	Id uint64
}

type RaftRPC interface {
	// Send a request to a peer to vote for us
	RequestVoteRPC(peer Peer, ballot Ballot) (BallotResponse, error)
	// Ask a peer to append entries to their log
	AppendEntriesRPC(peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error)
	// Forward a client request to the leader
	ForwardToLeaderRPC(peer Peer, req ClientRequest) (ClientRequestResponse, error)
	// Send a join cluster request to a peer
	JoinClusterRPC(peer Peer, req JoinClusterRequest) (JoinClusterResponse, error)
}
