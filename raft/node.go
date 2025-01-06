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
	ElectionTimeoutMin int
	ElectionTimeoutMax int
	HeartbeatTimeout   int
}

type NodeChannels struct {
	clientRequestChannel           chan ClientRequest
	clientResponseChannel          chan ClientRequestResponse
	electionChannel                chan BallotResponse
	requestVoteChannel             chan Ballot
	requestVoteResponseChannel     chan BallotResponse
	installSnapshotRequestChannel  chan InstallSnapshotRequest
	installSnapshotResponseChannel chan InstallSnapshotResponse
	appendEntriesRequestChannel    chan AppendEntriesRequest
	appendEntriesResponseChannel   chan AppendEntriesResponse
	addPeerChannel                 chan JoinClusterRequest
	JoinClusterResponseChannel     chan JoinClusterResponse
	clientReadRequestChannel       chan ClientReadRequest
	clientReadResponseChannel      chan ClientReadResponse
}
type ElectionState struct {
	votesReceived int
	electionTerm  uint64
}
type Node struct {
	Addr         string
	state        NodeState
	role         Role
	config       NodeConfig
	StateMachine StateMachine
	RaftRPC
	rpcStarted bool
	Peers      []Peer

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	leaderReplicationState map[uint64]FollowerReplicationState
	electionState          ElectionState
	channels               NodeChannels
	run                    bool

	*sync.Mutex
	clientRequestMutex *sync.Mutex
}

func NewNode(id uint64, addr string, rpcImplem RaftRPC, statemachine StateMachine, confDir string, config NodeConfig) *Node {
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
		role:           Follower,
		config:         config,
		StateMachine:   &DebugStateMachine{},
		RaftRPC:        rpcImplem,
		rpcStarted:     false,
		electionTimer:  time.NewTimer(time.Duration(rand.Intn(config.ElectionTimeoutMax)) * time.Millisecond),
		heartbeatTimer: time.NewTimer(1000 * time.Second),
		channels: NodeChannels{
			clientRequestChannel:           make(chan ClientRequest, 1),
			clientResponseChannel:          make(chan ClientRequestResponse, 1),
			electionChannel:                make(chan BallotResponse, 100),
			requestVoteChannel:             make(chan Ballot, 1),
			requestVoteResponseChannel:     make(chan BallotResponse, 1),
			installSnapshotRequestChannel:  make(chan InstallSnapshotRequest, 1),
			installSnapshotResponseChannel: make(chan InstallSnapshotResponse, 1),
			appendEntriesRequestChannel:    make(chan AppendEntriesRequest, 1),
			appendEntriesResponseChannel:   make(chan AppendEntriesResponse, 1),
			addPeerChannel:                 make(chan JoinClusterRequest, 1),
			JoinClusterResponseChannel:     make(chan JoinClusterResponse, 1),
			clientReadRequestChannel:       make(chan ClientReadRequest, 1),
			clientReadResponseChannel:      make(chan ClientReadResponse, 1),
		},
		Mutex:              &sync.Mutex{},
		clientRequestMutex: &sync.Mutex{},
	}
	return &node
}

func (n *Node) Start() {
	/* Start the node */
	n.run = true
	n.StartRPCServer()
	go n.nodeDaemon()
}

func (n *Node) StartRPCServer() {
	if n.rpcStarted {
		return
	}
	log.Println("Starting RPC server", n.rpcStarted, n.Addr)
	n.RaftRPC.RegisterNode(n)
	n.RaftRPC.Start()
	n.rpcStarted = true
}

func (n *Node) Stop() {
	n.run = false
}

func (n *Node) RestartElectionTimer() {
	n.StopElectionTimer()
	t := rand.Intn(n.config.ElectionTimeoutMax-n.config.ElectionTimeoutMin) + n.config.ElectionTimeoutMin
	n.electionTimer.Reset(time.Duration(t) * time.Millisecond)

}

func (n *Node) RestartHeartbeatTimer() {
	/* Restart the heartbeat timer for followers */
	n.StopElectionTimer()
	n.electionTimer.Reset(time.Duration(n.config.ElectionTimeoutMin) * time.Millisecond)
}

func (n *Node) forceNewElection() {
	/* Force a new election */
	n.StopElectionTimer()
	n.StopHeartbeatTimer()
	n.electionTimer.Reset(0 * time.Millisecond)
}

func (n *Node) StopElectionTimer() {
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
}

func (n *Node) StopHeartbeatTimer() {
	if !n.heartbeatTimer.Stop() {
		select {
		case <-n.heartbeatTimer.C:
		default:
		}
	}
}

func (n *Node) nodeDaemon() {
	/* Timer daemon */
	n.RestartElectionTimer()
	for n.run {
		select {
		case <-n.electionTimer.C:
			n.handleTimeout()
		case <-n.heartbeatTimer.C:
			// only the leader uses this timer
			if n.role == Leader {
				n.appendEntries()
			}
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
		case clientReadRequest := <-n.channels.clientReadRequestChannel:
			n.handleClientReadRequest(clientReadRequest)
		case installSnapshotRequest := <-n.channels.installSnapshotRequestChannel:
			n.handleInstallSnapshotRequest(installSnapshotRequest)
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
	if n.role == Candidate {
		n.loseElection()
		return
	}
	// follower
	n.StartElection()
}

func (n *Node) commitEntries() {
	/* Commit entries */
	for i := n.state.lastApplied + 1; i <= n.state.commitIndex; i++ {
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
			n.addPeerFromLog(logEntry)

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

func (n *Node) addPeerFromLog(logEntry LogEntry) error {
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
	n.addPeer(peer)
	return nil
}

func (n *Node) addPeer(peer Peer) error {
	/* Add a peer */
	existing := n.getPeer(peer.Id)
	if existing.Id != 0 {
		log.Printf("Node %d: peer %d %s already exists\n", n.state.id, peer.Id, peer.Addr)
		existing.Addr = peer.Addr
		return nil
	}
	n.Peers = append(n.Peers, peer)
	log.Printf("Node %d added peer %d %s\n", n.state.id, peer.Id, peer.Addr)
	return nil
}
