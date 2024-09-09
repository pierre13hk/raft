package raft

import (
	"encoding/json"
	"errors"
	"fmt"

	//"log"
	"math/rand"
	"time"
)

/* InMemoryLogger is a simple in-memory logger */
type InMemoryLogger struct {
	entries []LogEntry
}

func (l *InMemoryLogger) TruncateTo(index uint64) error {
	l.entries = l.entries[:index+1]
	return nil
}

func (l *InMemoryLogger) Cut(index uint64) error {
	l.entries = l.entries[index+1:]
	return nil
}

func (l *InMemoryLogger) LastLogTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

func (l *InMemoryLogger) LastLogIndex() uint64 {
	return l.entries[len(l.entries)-1].Index
}

func (l *InMemoryLogger) Get(index uint64) (LogEntry, error) {
	if index >= uint64(len(l.entries)) {
		str := fmt.Sprintf("No such entry, len in %d, index is %d", len(l.entries), index)
		return LogEntry{}, errors.New(str)
	}
	if index < 0 || index >= uint64(len(l.entries)) {
		return LogEntry{}, errors.New("No such entry")
	}
	return l.entries[index], nil
}

func (l *InMemoryLogger) GetRange(start uint64, end uint64) ([]LogEntry, error) {
	if start < 0 || start >= uint64(len(l.entries)) {
		return nil, errors.New("No such entry")
	}
	if end < 0 || end > uint64(len(l.entries))+1 {
		return nil, errors.New("No such entry")
	}
	return l.entries[start:end], nil
}

func (l *InMemoryLogger) GetFrom(start uint64) ([]LogEntry, error) {
	if uint64(len(l.entries)) < start {
		return nil, errors.New("No such entry")
	}
	return l.entries[start:], nil
}

func (l *InMemoryLogger) Append(entries []LogEntry) error {
	l.entries = append(l.entries, entries...)

	return nil
}

func (l *InMemoryLogger) Initialize(fileName string) error {
	return nil
}

func (l *InMemoryLogger) Commit(i uint64) error {
	return nil
}

func (l *InMemoryLogger) CreateSnapshot() error {
	return nil
}

/* Debug RPC */
type InMemoryRaftRPC struct {
	Peers map[uint64]*Node
}

func NewInMemoryRaftRPC() *InMemoryRaftRPC {
	return &InMemoryRaftRPC{
		Peers: make(map[uint64]*Node),
	}
}

func (r *InMemoryRaftRPC) RegisterNode(node *Node) {}
func (r *InMemoryRaftRPC) Start()                  {}

func (r *InMemoryRaftRPC) RequestVoteRPC(p Peer, ballot Ballot) (BallotResponse, error) {
	peer := r.Peers[p.Id]
	if peer == nil {
		return BallotResponse{}, errors.New("Peer not found")
	}

	// Simulate network latency
	//time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

	response := peer.HandleVoteRequest(ballot)

	return response, nil
}

func (r *InMemoryRaftRPC) AppendEntriesRPC(p Peer, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	peer := r.Peers[p.Id]
	if peer == nil {
		return AppendEntriesResponse{}, errors.New("Peer not found")
	}
	if rand.Float32() > 0.9 {
		return AppendEntriesResponse{}, errors.New("Random error")
	}
	//time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	// Simulate network latency

	resp := peer.RecvAppendEntries(req)
	return resp, nil
}

func (r *InMemoryRaftRPC) ForwardToLeaderRPC(peer Peer, req ClientRequest) (ClientRequestResponse, error) {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	// Simulate network latency
	node := r.Peers[peer.Id]
	return node.clientRequestHandler(req), nil
}

func (r *InMemoryRaftRPC) JoinClusterRPC(peer Peer, req JoinClusterRequest) (JoinClusterResponse, error) {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	// Simulate network latency
	return JoinClusterResponse{}, errors.New("Not implemented")
}

/* Debug StateMachine */
type DebugStateMachine struct {
	/* A simple state machine that logs all commands and can be used for testing */
	Log []string
}

func (d *DebugStateMachine) Apply(command []byte) error {
	d.Log = append(d.Log, string(command))
	return nil
}

func (d *DebugStateMachine) Serialize() ([]byte, error) {
	ser, err := json.Marshal(d.Log)
	if err != nil {
		return nil, err
	}
	return ser, nil
}

func (d *DebugStateMachine) Deserialize(data []byte) error {
	err := json.Unmarshal(data, &d.Log)
	if err != nil {
		return err
	}
	return nil
}

/* Debug Node */
type DebugNode struct {
	Node *Node
}

func NewDebugNode(id uint64, addr string, rpc RaftRPC) *DebugNode {
	return &DebugNode{
		Node: NewNode(id, addr, rpc),
	}
}

func (n *DebugNode) Start() {
	n.Node.Start()
}

func (n *DebugNode) Stop() []LogEntry {
	n.Node.Stop()
	entries, _ := n.Node.state.GetRange(0, n.Node.state.LastLogIndex()+1)
	return entries
}

func (n *DebugNode) Apply(command []byte) error {
	resp := n.Node.clientRequestHandler(ClientRequest{Command: command})
	if resp.Success {
		return nil
	} else {
		return errors.New("Failed to apply command")
	}
}
