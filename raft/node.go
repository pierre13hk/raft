package raft

type Role int
const (
	Follower Role = iota
	Candidate
	Leader
)

type NodeState struct {
	/* persistent state */
	currentTerm uint64
	votedFor uint64
	logger Logger

	/* volatile state */
	commitIndex uint64
	lastApplied uint64

	/* volatile state for leaders */
	nextIndex map[uint64]uint64
	matchIndex map[uint64]uint64
}

type Node struct {
	state NodeState
	role Role

	StateMachine StateMachine
	RaftRPC RaftRPC
}

func NewNode() *Node {
	return &Node{
		state: NodeState{
			currentTerm: 1,
			votedFor: 0,
			commitIndex: 0,
			lastApplied: 0,
			nextIndex: make(map[uint64]uint64),
			matchIndex: make(map[uint64]uint64),
			logger: &InMemoryLogger{},
		},
		role: Follower,
		StateMachine: &DebugStateMachine{},
		RaftRPC: &InMemoryRaftRPC{},
	}
}


func (n *Node) Start() {
	/* Start the node */
}

func (n *Node) AppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	/* AppendEntries RPC */
	return AppendEntriesResponse{}
}