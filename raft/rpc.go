package raft

import (
//"fmt"
)

type Peer struct {
	Id uint64
}

type RaftRPC interface {
	RequestVoteRPC(peer Peer, ballot Ballot, c chan BallotResponse) error
	AppendEntriesRPC(peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error)
	ForwardToLeaderRPC(peer Peer, req ClientRequest) (ClientRequestResponse, error)

}
