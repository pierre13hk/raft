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

func (client *RaftGrpcClient) AddClient(peer raft.Peer) error {
	connection, err := grpc.NewClient(peer.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	client.grpcClient = NewRaftClientServiceClient(connection)	
	if err != nil {
		return err
	}
	tmpClient := NewRaftClientServiceClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	_, err = tmpClient.AddClient(ctx, &emptypb.Empty{})
	if err != nil {
		log.Println("Client: failed to get cluster information: ", err)
		return err
	}
	return nil
}

func (client *RaftGrpcClient) Write(peer raft.Peer, request raft.ClientRequest) (raft.ClientRequestResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	grpcRequest := &WriteRequest{
		Command: string(request.Command),
	}
	_, err := client.grpcClient.Write(ctx, grpcRequest)
	if err != nil {
		return raft.ClientRequestResponse{Success: false}, err
	}
	return raft.ClientRequestResponse{Success: true}, nil
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
		Error: grpcResponse.Error,
	}, nil
}

func (client *RaftGrpcClient) setClient() error {
	connection, err := grpc.NewClient(client.leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	client.grpcClient = NewRaftClientServiceClient(connection)
	return err
}
