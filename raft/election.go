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

func (n *Node) RequestVote(ballot Ballot) BallotResponse {
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
	return BallotResponse{Term: ballot.Term, VoteGranted: true}
}

func (n *Node) StartElection() {
	log.Printf("Node %d starting election", n.state.id)
	n.mtx.Lock()
	n.state.currentTerm += 1
	n.state.votedFor = n.state.id

	go n.state.save()

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
			err := n.RaftRPC.RequestVoteRPC(p, ballot, n.electionChannel)
			if err != nil {
				log.Printf("Error sending RequestVote rpc to %d: %v", p.Id, err)
			}
		}(peer)
	}

	/* Count the votes */
	log.Printf("Node %d waiting for votes term: %d\n", n.state.id, n.state.currentTerm)
	n.RestartElectionTimer()
	votes := 1
	for range n.Peers {
		resp, open := <-n.electionChannel
		if !open {
			/* Our channel is closed, the election is over */
			n.role = Follower
			n.state.votedFor = 0
			n.RestartElectionTimer()
			return
		}

		if resp.VoteGranted && resp.Term == n.state.currentTerm {
			log.Printf("Node %d received vote\n", n.state.id)
			votes++
		}
		if votes > len(n.Peers)/2 {
			/* Won the election */
			n.role = Leader
			log.Printf("Node %d won the election", n.state.id)
			n.timer.Stop()
			go n.leaderDaemon()
			close(n.electionChannel)
			return
		}
	}
	/* Lost the election */
	log.Printf("Node %d lost the election w votes %d\n", n.state.id, votes)
	n.role = Follower
	n.state.votedFor = 0
	close(n.electionChannel)
	n.RestartElectionTimer()
}
