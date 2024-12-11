package raft

import "errors"

var (
	FailedToFindLeaderErr = errors.New("failed to find leader")
)

type RaftGrpcClient struct {
	leaderId   uint64
	leaderAddr string
}

type RaftClientStub interface {
	Init() error
	AddClient(peer Peer) error
	Write(peer Peer, request ClientRequest) (ClientRequestResponse, error)
	Read(peer Peer, request ClientReadRequest) (ClientReadResponse, error)
}

type RaftClient struct {
	leader Peer
	peers  []Peer
	stub   RaftClientStub
}

func NewRaftClient(stub RaftClientStub, peers ...Peer) RaftClient {
	return RaftClient{
		leader: Peer{},
		stub:   stub,
		peers:  peers,
	}
}

func (client *RaftClient) Init() error {
	if err := client.stub.Init(); err != nil {
		return err
	}
	return client.FindLeader()
}

func (client *RaftClient) FindLeader() error {
	if client.leader != (Peer{}) {
		if client.stub.AddClient(client.leader) == nil {
			// our leader is still the leader
			return nil
		}
	}
	for _, peer := range client.peers {
		err := client.stub.AddClient(peer)
		if err != nil {
			continue
		}
		client.leader = peer
		return nil
	}
	return FailedToFindLeaderErr
}

func (client *RaftClient) Write(command string) (ClientRequestResponse, error) {
	request := ClientRequest{Command: []byte(command)}
	errorFindingLeader := client.FindLeader()
	if errorFindingLeader != nil {
		return ClientRequestResponse{}, errorFindingLeader
	}
	return client.stub.Write(client.leader, request)
}

func (client *RaftClient) Read(request ClientReadRequest) (ClientReadResponse, error) {
	errorFindingLeader := client.FindLeader()
	if errorFindingLeader != nil {
		return ClientReadResponse{}, errorFindingLeader
	}
	return client.stub.Read(client.leader, request)
}
