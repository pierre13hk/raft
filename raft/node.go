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
	installSnapshotChannel     chan InstallSnapshotRequest
}
type ElectionState struct {
	votesReceived int
	electionTerm  uint64
}
type Node struct {
	Addr  string
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

	*sync.Mutex
}

func NewNode(id uint64, addr string, rpcImplem RaftRPC, statemachine StateMachine, confDir string) *Node {
	logger := NewLoggerImplem(
		statemachine,
		confDir,
		'\n',
	)
	appendErr := logger.Append([]LogEntry{{
		Term:    0,
		Index:   0,
		Type:    RAFT_LOG,
		Command: []byte("logger init"),
	}})
	if appendErr != nil {
		log.Println("Error initializing logger")
	}
	node := Node{
		Addr: addr,
		state: NodeState{
			id:          id,
			currentTerm: 1,
			votedFor:    0,
			commitIndex: 0,
			lastApplied: 0,
			Logger:      logger,
		},
		role:         Follower,
		StateMachine: &DebugStateMachine{},
		RaftRPC:      rpcImplem,
		timer:        time.NewTimer(time.Duration(200+rand.Intn(150)) * time.Millisecond),
		channels: NodeChannels{
			requestVoteChannel:         make(chan Ballot, 100),
			requestVoteResponseChannel: make(chan BallotResponse, 100),
			installSnapshotChannel:     make(chan InstallSnapshotRequest, 100),
		},
		Mutex: &sync.Mutex{},
	}
	node.RaftRPC.RegisterNode(&node)
	node.RaftRPC.Start()
	return &node
}

func (n *Node) Start() {
	/* Start the node */
	n.run = true
	go n.nodeDaemon()
	log.Println("Node started")
}

func (n *Node) Stop() {
	n.run = false
}

func (n *Node) RestartElectionTimer() {
	n.StopTimer()
	t := rand.Intn(4000)
	n.timer.Reset(time.Duration(t) * time.Millisecond)

}

func (n *Node) RestartHeartbeatTimer() {
	/* Restart the heartbeat timer */
	n.StopTimer()
	n.timer.Reset(time.Duration(1) * time.Second)
}

func (n *Node) RestartLeaderHeartBeatTimer() {
	/* Restart the heartbeat timer */
	n.StopTimer()
	n.timer.Reset(time.Duration(200) * time.Millisecond)
}

func (n *Node) StopTimer() {
	if !n.timer.Stop() {
		select {
		case <-n.timer.C:
		default:
		}
	}
}

func (n *Node) nodeDaemon() {
	/* Timer daemon */
	n.RestartElectionTimer()
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
	return Peer{n.state.id, "la"}, nil
}

func (n *Node) handleTimeout() {
	if n.role == Leader {
		n.appendEntries()
		return
	}
	if n.role == Candidate {
		n.loseElection()
		return
	}
	// follower
	n.StartElection()
}

func (n *Node) commitEntries() {
	/* Commit entries */
	for i := n.state.lastApplied; i <= n.state.commitIndex; i++ {
		logEntry, err := n.state.Get(i)
		if err != nil {
			log.Println("Error getting log entry")
			return
		}
		switch logEntry.Type {
		case USER_LOG:
			n.StateMachine.Apply(logEntry.Command)
		// Configurations changes
		case RAFT_LOG:
			// do nothing
		case CLUSTER_CHANGE_ADD:
			//
		}
	}
	log.Printf("Node %d committed entries from  %d %d\n",
		n.state.id,
		n.state.lastApplied,
		n.state.commitIndex,
	)
	n.state.lastApplied = n.state.commitIndex
	n.state.commitIndex = n.state.LastLogIndex()
}
