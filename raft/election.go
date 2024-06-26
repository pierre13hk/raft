package raft

import (
	"log"
)

type Ballot struct {
	/* Struct for RequestVote rpc*/
	Term         uint64
	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type BallotResponse struct {
	/* Struct for RequestVote rpc response*/
	Term        uint64
	VoteGranted bool
}

func (n *Node) HandleVoteRequest(ballot Ballot) BallotResponse {
	/*
	 * Run by a node when it receives a RequestVote rpc
	 */
	log.Printf("Node %d received RequestVote from %d, local term %d vs ballot term %d\n", n.state.id, ballot.CandidateId, n.state.currentTerm, ballot.Term)
	/* We have a newer term */
	if ballot.Term < n.state.currentTerm {
		return BallotResponse{Term: n.state.currentTerm, VoteGranted: false}
	}

	if n.state.votedFor != 0 {
		/* Already voted for someone else */
		return BallotResponse{Term: n.state.currentTerm, VoteGranted: false}
	}

	if ballot.LastLogTerm < n.state.logger.LastLogTerm() {
		return BallotResponse{Term: n.state.currentTerm, VoteGranted: false}
	}

	if ballot.LastLogTerm == n.state.logger.LastLogTerm() && ballot.LastLogIndex < n.state.logger.LastLogIndex() {
		/* Candidate's log less up-to-date than ours*/
		return BallotResponse{Term: n.state.currentTerm, VoteGranted: false}
	}
	/* Vote for the candidate
	 * The ballot's term is at least as large as the current term
	 * and the candidate's log is at least as up-to-date as ours
	 * and we haven't voted for anyone else
	 */
	n.state.votedFor = ballot.CandidateId
	n.state.currentTerm = ballot.Term
	n.role = Follower
	go n.state.save()
	n.RestartElectionTimer()
	log.Printf("Node %d voted for %d\n", n.state.id, ballot.CandidateId)
	return BallotResponse{Term: ballot.Term, VoteGranted: true}
}

func (n *Node) StartElection() {
	log.Printf("Node %d starting election\n", n.state.id)
	n.mtx.Lock()
	n.StopTimer()
	n.state.currentTerm += 1
	n.state.votedFor = n.state.id
	n.role = Candidate
	n.state.save()

	n.role = Candidate
	ballot := Ballot{
		Term:         n.state.currentTerm,
		CandidateId:  n.state.id,
		LastLogIndex: n.state.logger.LastLogIndex(),
		LastLogTerm:  n.state.logger.LastLogTerm(),
	}
	n.mtx.Unlock()

	/* Send RequestVote rpc to all other nodes */

	n.electionChannel = make(chan BallotResponse, len(n.Peers))
	for _, peer := range n.Peers {
		if peer.Id == n.state.id {
			continue
		}
		go func(p Peer) {
			_, err := n.RequestVoteRPC(p, ballot)
			if err != nil {
				log.Printf("Error sending RequestVote rpc to %d: %v", p.Id, err)
			}
		}(peer)
	}
	log.Printf("Node %d sent RequestVote rpcs, collecting votes\n", n.state.id)
}

func (n *Node) requestVote(peer Peer, ballot Ballot) {
	ballotResponse, err := n.RequestVoteRPC(peer, ballot)
	if err != nil {
		n.channels.requestVoteResponseChannel <- ballotResponse
	} else {
		log.Printf("Error sending RequestVote rpc to %d: %v", peer.Id, err)
	}
}

func (n *Node) HandleVoteRequestResponse(ballot BallotResponse) {
	/*
	 * Run by a candidate to collect votes and determine if it has won the election
	 */
	if ballot.Term == n.electionState.electionTerm {
		if ballot.VoteGranted {
			n.electionState.votesReceived += 1
			if n.electionState.votesReceived > len(n.Peers)/2 {
				/* Won the election */
			}
		}
	}
	// if the ballot term is not in the current election term, ignore the vote
}
