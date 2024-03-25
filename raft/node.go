package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type NodeState struct {
	/* persistent state */
	id          uint64
	currentTerm uint64
	votedFor    uint64
	logger      Logger

	/* volatile state */
	commitIndex uint64
	lastApplied uint64

	/* volatile state for leaders */
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64
}

func (n *NodeState) save() {
	// todo: save to disk
}

type NodeConfig struct {
	avgTimeout    int
	timeoutWindow int
}

type Node struct {
	state NodeState
	role  Role

	StateMachine StateMachine
	RaftRPC      RaftRPC
	Peers        []Peer

	electionChannel chan BallotResponse
	mtx             sync.Mutex
	timer           *time.Timer
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderId     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

func NewNode(id uint64) *Node {
	return &Node{
		state: NodeState{
			id:          id,
			currentTerm: 1,
			votedFor:    0,
			commitIndex: 0,
			lastApplied: 0,
			nextIndex:   make(map[uint64]uint64),
			matchIndex:  make(map[uint64]uint64),
			logger: &InMemoryLogger{
				entries: []LogEntry{
					{Term: 1, Index: 1, Command: []byte("init")},
				},
			},
		},
		role:         Follower,
		StateMachine: &DebugStateMachine{},
		RaftRPC:      &InMemoryRaftRPC{},
		timer:        time.NewTimer(time.Duration(200+rand.Intn(150)) * time.Millisecond),
	}
}

func (n *Node) Start() {
	/* Start the node */
	go n.nodeDaemon()
	
}

func (n *Node) AppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	/* AppendEntries RPC */

	if req.Term < n.state.currentTerm {
		log.Println("AppendEntries: Request term is less than current term")
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: false}
	}
	lg, err := n.state.logger.Get(req.PrevLogIndex)
	if err != nil || lg.Term != req.PrevLogTerm {
		log.Println("AppendEntries: Log doesn't match")
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: false}
	}

	if len(req.Entries) == 0 {
		// heatbeat
		log.Println("AppendEntries: Heartbeat from leader")
		n.RestartElectionTimer()
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: true}
	}

	logs, _ := n.state.logger.GetRange(req.PrevLogIndex + 1)
	for i, entry := range logs {
		// A node can't have a more up to date log than the leader
		// So req.Entries[i] should always be valid / in bounds
		if entry.Term != req.Entries[i].Term {
			log.Println("AppendEntries: Log doesn't match, truncating from ", entry.Index, entry.Command)
			n.state.logger.TruncateTo(entry.Index)
			break
		}
	}
	log.Println("AppendEntries: Appending new entries")
	new := 0
	for i, entry := range req.Entries {
		_, err := n.state.logger.Get(entry.Index)
		if err != nil {
			new = i
			break
		}
	}

	n.state.logger.Append(req.Entries[new:])

	return AppendEntriesResponse{n.state.currentTerm, true}
}

func (n *Node) RestartElectionTimer() {
	/* Restart the election timer */
	n.timer.Reset(time.Duration(200+rand.Intn(150)) * time.Millisecond)
}

func (n *Node) nodeDaemon() {
	/* Timer daemon */
	for {
		select {
		case <-n.timer.C:
			// Election timeout
			n.StartElection()
		}
	}
}
