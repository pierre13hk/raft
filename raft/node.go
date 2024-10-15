package raft

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
	ElectionLoser
	AddingPeer
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
	clientRequestChannel         chan ClientRequest
	clientResponseChannel        chan ClientRequestResponse
	electionChannel              chan BallotResponse
	requestVoteChannel           chan Ballot
	requestVoteResponseChannel   chan BallotResponse
	installSnapshotChannel       chan InstallSnapshotRequest
	appendEntriesRequestChannel  chan AppendEntriesRequest
	appendEntriesResponseChannel chan AppendEntriesResponse
	addPeerChannel               chan JoinClusterRequest
	JoinClusterResponseChannel   chan JoinClusterResponse
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

	timer               *time.Timer
	timerBackoffCounter int

	leaderReplicationState map[uint64]FollowerReplicationState
	electionState          ElectionState
	channels               NodeChannels
	run                    bool

	*sync.Mutex
	clientRequestMutex *sync.Mutex
}

func NewNode(id uint64, addr string, rpcImplem RaftRPC, statemachine StateMachine, confDir string) *Node {
	logger := NewLoggerImplem(
		statemachine,
		confDir,
		'\n',
	)
	if logger.Empty() {
		appendErr := logger.Append([]LogEntry{{
			Term:    0,
			Index:   0,
			Type:    RAFT_LOG,
			Command: []byte("logger init"),
		}})
		if appendErr != nil {
			log.Println("Error initializing logger")
		}
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
			clientRequestChannel:         make(chan ClientRequest, 1),
			clientResponseChannel:        make(chan ClientRequestResponse, 1),
			electionChannel:              make(chan BallotResponse, 100),
			requestVoteChannel:           make(chan Ballot, 1),
			requestVoteResponseChannel:   make(chan BallotResponse, 1),
			installSnapshotChannel:       make(chan InstallSnapshotRequest, 100),
			appendEntriesRequestChannel:  make(chan AppendEntriesRequest, 1),
			appendEntriesResponseChannel: make(chan AppendEntriesResponse, 1),
			addPeerChannel:               make(chan JoinClusterRequest, 1),
			JoinClusterResponseChannel:   make(chan JoinClusterResponse, 1),
		},
		Mutex:              &sync.Mutex{},
		clientRequestMutex: &sync.Mutex{},
	}
	node.RaftRPC.RegisterNode(&node)
	node.RaftRPC.Start()
	return &node
}

func (n *Node) Start() {
	/* Start the node */
	n.run = true
	wg := sync.WaitGroup{}
	go n.nodeDaemon()
	wg.Add(1)
	wg.Wait()
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
		case clientRequest := <-n.channels.clientRequestChannel:
			n.write(clientRequest)
		case ballot := <-n.channels.requestVoteChannel:
			// when any node receives a vote request
			n.handleVoteRequest(ballot)
		case ballotResponse := <-n.channels.electionChannel:
			// when a candidate collects votes
			n.HandleVoteRequestResponse(ballotResponse)
		case appendEntriesRequest := <-n.channels.appendEntriesRequestChannel:
			// when a node, normally a folower, receives append entries request
			n.handleRecvAppendEntries(appendEntriesRequest)
		case JoinClusterRequest := <-n.channels.addPeerChannel:
			// when a node receives a request to add a peer
			n.handleJoinClusterRequest(JoinClusterRequest)
		}

	}
}

func (n *Node) GetLeader() (Peer, error) {
	/* Get the leader */
	if n.role == Leader {
		return Peer{
			Id:   n.state.id,
			Addr: n.Addr,
		}, nil
	}
	if n.role == Follower {
		return *n.getPeer(n.state.votedFor), nil
	}
	return Peer{}, errors.New("No leader")
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
			n.addPeer(logEntry)

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

func (n *Node) getPeer(peerId uint64) *Peer {
	for _, peer := range n.Peers {
		if peer.Id == peerId {
			return &peer
		}
	}
	return &Peer{}
}

func (n *Node) GetLeaderInfo() (uint64, string) {
	/* Get leader info */
	leader, err := n.GetLeader()
	if err != nil {
		return 0, ""
	}
	return leader.Id, leader.Addr
}

func (n *Node) addPeer(logEntry LogEntry) error {
	/* Add a peer */
	if logEntry.Type != CLUSTER_CHANGE_ADD {
		return errors.New("Wrong log type")
	}
	peerStrInfo := logEntry.Command
	peerInfo := strings.Split(string(peerStrInfo), RAFT_COMMAND_DELIMITER)
	if len(peerInfo) != 3 {
		log.Println("Error adding peer, want id,ip_addr,port")
		return errors.New("Error adding peer, want id,ip_addr,port")
	}
	id, err := strconv.ParseUint(peerInfo[0], 10, 64)
	if err != nil {
		log.Println("Error parsing peer id")
		return errors.New("Error parsing peer id")
	}
	if ip := net.ParseIP(peerInfo[1]); ip == nil {
		log.Println("Error parsing peer address", peerInfo[1])
		return errors.New("Error parsing peer address")
	}
	port, err := strconv.Atoi(peerInfo[2])
	if err != nil {
		log.Println("Error parsing peer port")
		return errors.New("Error parsing peer port")
	}
	addr := peerInfo[1] + ":" + strconv.Itoa(port)
	peer := Peer{
		Id:   id,
		Addr: addr,
	}
	existing := n.getPeer(id)
	if existing.Id != 0 {
		log.Printf("Node %d: peer %d %s already exists\n", n.state.id, id, addr)
		existing.Addr = addr
		return nil
	}
	n.Peers = append(n.Peers, peer)
	log.Printf("Node %d added peer %d %s\n", n.state.id, id, addr)
	if n.role == Leader {
		n.leaderReplicationState[id] = FollowerReplicationState{
			nextIndex:  n.state.LastLogIndex() + 1,
			matchIndex: 0,
		}
	}
	return nil
}
