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

func (n *Node) checkVoteRequest(ballot Ballot) BallotResponse {
	/*
	 * Determine if we should vote for the candidate given the ballot
	 */
	log.Printf("Node %d received RequestVote from %d, local term %d vs ballot term %d\n", n.state.id, ballot.CandidateId, n.state.currentTerm, ballot.Term)
	/* We have a newer term */
	if ballot.Term < n.state.currentTerm {
		return BallotResponse{Term: n.state.currentTerm, VoteGranted: false}
	}

	if n.state.votedFor != 0 && n.state.votedFor != n.state.id {
		/* Already voted for someone else */
		log.Printf("Node %d received RequestVote from %d,already voted\n", n.state.id, ballot.CandidateId)
		return BallotResponse{Term: n.state.currentTerm, VoteGranted: false}
	}

	if ballot.LastLogTerm < n.state.LastLogTerm() {
		log.Printf("Node %d received RequestVote from %d, ,term log not up to date\n", n.state.id, ballot.CandidateId)
		return BallotResponse{Term: n.state.currentTerm, VoteGranted: false}
	}

	if ballot.LastLogTerm == n.state.LastLogTerm() && ballot.LastLogIndex < n.state.LastLogIndex() {
		/* Candidate's log less up-to-date than ours*/
		log.Printf("Node %d received RequestVote from %d,log index late \n", n.state.id, ballot.CandidateId)
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
	n.RestartHeartbeatTimer()
	log.Printf("Node %d voted for %d\n", n.state.id, ballot.CandidateId)
	return BallotResponse{Term: ballot.Term, VoteGranted: true}
}

func (n *Node) handleVoteRequest(ballot Ballot) {
	/*
	 * Run by a candidate when it receives a vote
	 */
	n.channels.requestVoteResponseChannel <- n.checkVoteRequest(ballot)
}

func (n *Node) RecvVoteRequest(ballot Ballot) BallotResponse {
	/*
	 * Called by the RPC layer when a node receives a RequestVote rpc
	 */
	n.channels.requestVoteChannel <- ballot
	return <-n.channels.requestVoteResponseChannel
}

func (n *Node) StartElection() {
	log.Printf("Node %d starting election\n", n.state.id)
	n.StopElectionTimer()
	n.state.currentTerm += 1
	n.electionState.electionTerm = n.state.currentTerm
	n.electionState.votesReceived = 1
	n.state.votedFor = n.state.id
	n.role = Candidate
	n.state.save()

	ballot := Ballot{
		Term:         n.state.currentTerm,
		CandidateId:  n.state.id,
		LastLogIndex: n.state.LastLogIndex(),
		LastLogTerm:  n.state.LastLogTerm(),
	}

	/* Send RequestVote rpc to all other nodes */

	for _, peer := range n.Peers {
		if peer.Id == n.state.id {
			continue
		}
		go n.requestVote(peer, ballot)
	}
	log.Printf("Node %d sent RequestVote rpcs, collecting votes\n", n.state.id)
	n.RestartElectionTimer()
}

func (n *Node) requestVote(peer Peer, ballot Ballot) {
	ballotResponse, err := n.RequestVoteRPC(peer, ballot)
	if err == nil {
		n.channels.electionChannel <- ballotResponse
	} else {
		log.Println("Error sending RequestVote rpc to ", peer.Id, err)
	}
}

func (n *Node) handleVoteRequestResponse(ballot BallotResponse) {
	/*
	 * Run by a candidate to collect votes and determine if it has won the election
	 */
	if ballot.Term == n.electionState.electionTerm {
		if ballot.VoteGranted {
			n.electionState.votesReceived += 1
			if n.electionState.votesReceived > len(n.Peers)/2 {
				/* Won the election */
				if n.role == Candidate {
					log.Printf("Node %d won the election\n", n.state.id)
					n.becomeLeader()
				}
			}
		}
	}
	// if the ballot term is not in the current election term, ignore the vote
}

func (n *Node) loseElection() {
	/*
	 * Run by a candidate when it loses the election
	 */
	log.Printf("Node %d lost the election\n", n.state.id)
	n.role = Follower
	n.state.votedFor = 0
	n.RestartHeartbeatTimer()
}
