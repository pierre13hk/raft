package grpc

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	raft "raft.com/raft"
)

var TIMEOUT = 1000 * time.Millisecond

type RaftGrpcClient struct {
	grpcClient RaftClientServiceClient
	leaderId   uint64
	leaderAddr string
	servers    []string
}

func NewRaftGrpcClient() *RaftGrpcClient {
	return &RaftGrpcClient{
		leaderId:   0,
		leaderAddr: "",
	}
}

func (client *RaftGrpcClient) Init() error {
	return nil
}

func (client *RaftGrpcClient) AddClient(peer raft.Peer) (raft.ClusterInfo, error) {
	connection, err := grpc.NewClient(peer.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client.grpcClient = NewRaftClientServiceClient(connection)
	if err != nil {
		return raft.ClusterInfo{}, err
	}
	tmpClient := NewRaftClientServiceClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	resp, err := tmpClient.AddClient(ctx, &emptypb.Empty{})
	if err != nil {
		log.Println("Client: failed to get cluster information: ", err)
		return raft.ClusterInfo{}, err
	}
	return raft.ClusterInfo{IsLeader: resp.IsLeader}, nil
}

func (client *RaftGrpcClient) Write(peer raft.Peer, request raft.ClientRequest) (raft.ClientRequestResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	grpcRequest := &WriteRequest{
		Command: string(request.Command),
	}
	response, err := client.grpcClient.Write(ctx, grpcRequest)
	if err != nil {
		return raft.ClientRequestResponse{Success: false}, err
	}
	return raft.ClientRequestResponse{Success: response.Success}, nil
}

func (client *RaftGrpcClient) Read(peer raft.Peer, request raft.ClientReadRequest) (raft.ClientReadResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	grpcRequest := &ReadRequest{Command: string(request.Command)}
	grpcResponse, err := client.grpcClient.Read(ctx, grpcRequest)
	if err != nil {
		log.Println("Client: failed to read logs: ", err, " leader addr ", client.leaderAddr)
		return raft.ClientReadResponse{Success: false, Error: err.Error()}, err
	}
	return raft.ClientReadResponse{
		Response: []byte(grpcResponse.Response),
		Success:  grpcResponse.Success,
		Error:    grpcResponse.Error,
	}, nil
}

