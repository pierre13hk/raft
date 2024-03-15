package raft

type Ballot struct {
	/* Struct for RequestVote rpc*/
	Term uint64
	CandidateId uint64
	LastLogIndex uint64
	LastLogTerm uint64
}

type BallotResponse struct {
	/* Struct for RequestVote rpc response*/
	Term uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term uint64
	LeaderId uint64
	PrevLogIndex uint64
	PrevLogTerm uint64
	Entries []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term uint64
	Success bool
}


func (n *Node) RequestVote(ballot Ballot) BallotResponse {
	/* RequestVote RPC */
	return BallotResponse{}
}