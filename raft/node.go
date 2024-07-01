package raft

import (
	"errors"
	"log"
	"math/rand"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
	ElectionLoser
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
	Logger

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

type NodeChannels struct {
	requestVoteChannel         chan Ballot
	requestVoteResponseChannel chan BallotResponse
	appendEntriesResponse      chan AppendEntriesResponse
}
type ElectionState struct {
	votesReceived int
	electionTerm  uint64
}
type Node struct {
	state NodeState
	role  Role

	StateMachine StateMachine
	RaftRPC
	Peers []Peer

	electionChannel     chan BallotResponse
	timer               *time.Timer
	timerBackoffCounter int

	leaderReplicationState map[uint64]FollowerReplicationState
	electionState          ElectionState
	channels               NodeChannels
	run                    bool
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
			Logger: &InMemoryLogger{
				entries: []LogEntry{
					{Term: 1, Index: 0, Command: []byte("init")},
				},
			},
		},
		role:         Follower,
		StateMachine: &DebugStateMachine{},
		RaftRPC:      &InMemoryRaftRPC{},
		timer:        time.NewTimer(time.Duration(200+rand.Intn(150)) * time.Millisecond),
		channels: NodeChannels{
			requestVoteResponseChannel: make(chan BallotResponse, 10),
			appendEntriesResponse:      make(chan AppendEntriesResponse, 10),
		},
	}
}

func (n *Node) Start() {
	/* Start the node */
	n.run = true
	go n.nodeDaemon()
}

func (n *Node) Stop() {
	n.run = false
}

func (n *Node) recvAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	/* AppendEntries RPC */
	if req.Term < n.state.currentTerm {
		log.Printf("Node %d: AppendEntries: Term %d < currentTerm %d\n", n.state.id, req.Term, n.state.currentTerm)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: false}
	}
	if n.state.currentTerm <= req.Term && n.role == Candidate {
		log.Printf("Node %d: AppendEntries: Term %d >= currentTerm %d, stepping down\n", n.state.id, req.Term, n.state.currentTerm)
		n.role = Follower
		n.state.currentTerm = req.Term
		n.state.votedFor = req.LeaderId
		close(n.electionChannel)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: true}
	}

	n.RestartHeartbeatTimer()
	lg, err := n.state.Get(req.PrevLogIndex)
	if err != nil || lg.Term != req.PrevLogTerm {
		log.Println("AppendEntries: Log doesn't match", err, lg.Term, req.PrevLogTerm)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: false}
	}

	if n.role == Candidate {
		close(n.electionChannel)
	}
	n.role = Follower
	n.state.votedFor = req.LeaderId
	if len(req.Entries) == 0 {
		// heartbeat
		n.timerBackoffCounter = 1
		log.Printf("Node %d: AppendEntries: Heartbeat from leader %d\n", n.state.id, req.LeaderId)
		return AppendEntriesResponse{Term: n.state.currentTerm, Success: true}
	}

	//logs, _ := n.state.GetRange(req.PrevLogIndex + 1)
	if req.Entries[0].Index <= n.state.LastLogIndex()-1 {
		for _, entry := range req.Entries {
			if entry.Index > n.state.LastLogIndex() {
				break
			}
			localLog, err := n.state.Get(entry.Index)
			if err != nil {
				log.Println("shouldnt happen")
				break
			}
			// A node can't have a more up to date log than the leader
			// So req.Entries[i] should always be valid / in bounds
			if entry.Term != localLog.Term {
				log.Println("Node % d AppendEntries: Log doesn't match, truncating from ", n.state.id, entry.Index, entry.Command)
				n.state.TruncateTo(entry.Index)
				break
			}
		}
	}

	new := 0
	for i, entry := range req.Entries {
		_, err := n.state.Get(entry.Index)
		if err != nil {
			new = i
			break
		}
	}
	log.Printf("Node %d: AppendEntries: Appending %d new entries new = %d, len reqent %d\n", n.state.id, len(req.Entries[new:]), new, len(req.Entries))
	n.state.Append(req.Entries[new:])

	return AppendEntriesResponse{n.state.currentTerm, true}
}

func (n *Node) RestartElectionTimer() {
	/* Restart the election timer */
	if !n.timer.Stop() {
	}
	t := time.Duration(1) * time.Second
	n.timer.Reset(t)
}

func (n *Node) RestartHeartbeatTimer() {
	/* Restart the heartbeat timer */
	n.timer.Stop()
	n.timer.Reset(time.Duration(1) * time.Second)
}

func (n *Node) StopTimer() {
	n.timer.Stop()
}

func (n *Node) nodeDaemon() {
	/* Timer daemon */
	for n.run {
		select {
		case <-n.timer.C:
			n.handleTimeout()
		case ballot := <-n.channels.requestVoteChannel:
			n.HandleVoteRequest(ballot)
		case ballotResponse := <-n.channels.requestVoteResponseChannel:
			n.HandleVoteRequestResponse(ballotResponse)
		}
	}
}

func (n *Node) GetLeader() (Peer, error) {
	/* Get the leader */
	if n.role != Leader {
		for _, peer := range n.Peers {
			if peer.Id == n.state.votedFor {
				return peer, nil
			}
		}
	}
	return Peer{}, errors.New("No leader")
}

func (n *Node) handleTimeout() {
	log.Println("Node ", n.state.id, " timeout")
}
