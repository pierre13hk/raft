package grpc

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var TIMEOUT = 1000 * time.Millisecond

type RaftGrpcClient struct {
	grpcClient RaftClientServiceClient
	leaderId   uint64
	leaderAddr string
	servers    []string
}

func NewRaftGrpcClient() RaftGrpcClient {
	return RaftGrpcClient{
		leaderId:   0,
		leaderAddr: "",
	}
}

func (client *RaftGrpcClient) setClient() error {
	connection, err := grpc.NewClient(client.leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	client.grpcClient = NewRaftClientServiceClient(connection)
	return err
}

func (client *RaftGrpcClient) ConnectToCluster(servers []string) error {
	client.servers = servers
	for _, server := range servers {
		connection, err := grpc.NewClient(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		tmpClient := NewRaftClientServiceClient(connection)
		ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
		defer cancel()
		clusterInfo, err := tmpClient.AddClient(ctx, &emptypb.Empty{})
		if err != nil {
			log.Println("Client: failed to get cluster information: ", err)
			continue
		}
		client.leaderId = clusterInfo.LeaderId
		client.leaderAddr = clusterInfo.LeaderAddr
		client.grpcClient = tmpClient
		log.Println("Client: connected to cluster with leader: ", client.leaderId, " at ", client.leaderAddr)
		return client.setClient()
	}
	return nil
}

func (client *RaftGrpcClient) Write(request string) WriteResponse {
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	grpcRequest := &WriteRequest{
		Command: request,
	}
	grpcResponse, err := client.grpcClient.Write(ctx, grpcRequest)
	if err != nil {
		log.Println("Client: failed to write command: ", err, " leader addr ", client.leaderAddr)
		return WriteResponse{Success: false}
	}
	return WriteResponse{Success: grpcResponse.Success}
}
