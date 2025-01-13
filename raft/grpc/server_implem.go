package grpc

import (
	"context"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	rft "raft.com/raft"
)

type RaftRpcImplem struct {
	UnimplementedRaftNodeServer
	UnimplementedRaftClientServiceServer
	node   *rft.Node
	server *grpc.Server
}

func (server *RaftRpcImplem) RegisterNode(node *rft.Node) {
	server.node = node
	log.Printf("Node %d registered with RPC layer\n", node.Addr)
}

func (server *RaftRpcImplem) newGRPCClient(peer rft.Peer) (RaftNodeClient, error) {
	connection, err := grpc.NewClient(peer.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to %s\n", peer.Addr)
		return nil, err
	}
	return NewRaftNodeClient(connection), nil
}

func (server *RaftRpcImplem) Start() {
	if server.node == nil {
		panic("Node not registered")
	}
	lis, err := net.Listen("tcp", server.node.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Starting RPC server listening on %s\n", server.node.Addr)
	grpcServer := grpc.NewServer()
	RegisterRaftNodeServer(grpcServer, server)
	RegisterRaftClientServiceServer(grpcServer, server)
	go grpcServer.Serve(lis)
	server.server = grpcServer
	log.Printf("RPC server started\n")
}

func (server *RaftRpcImplem) Stop() {
	if server.node == nil {
		panic("Node not registered")
	}
	server.server.Stop()
}

// raft RPC interface implementations, really just a wrapper for the grpc client
func (server *RaftRpcImplem) RequestVoteRPC(peer rft.Peer, ballot rft.Ballot) (rft.BallotResponse, error) {
	grpcBallot := &RPCBallot{
		Term:         ballot.Term,
		CandidateId:  ballot.CandidateId,
		LastLogIndex: ballot.LastLogIndex,
		LastLogTerm:  ballot.LastLogTerm,
	}
	client, err := server.newGRPCClient(peer)
	if err != nil {
		return rft.BallotResponse{}, err
	}
	grpcBallotResponse, err := client.RequestVote(context.Background(), grpcBallot)
	if err != nil {
		return rft.BallotResponse{}, err
	}
	raftBallotResponse := rft.BallotResponse{
		Term:        grpcBallotResponse.Term,
		VoteGranted: grpcBallotResponse.VoteGranted,
	}
	return raftBallotResponse, nil
}

func (server *RaftRpcImplem) AppendEntriesRPC(peer rft.Peer, request rft.AppendEntriesRequest) (rft.AppendEntriesResponse, error) {
	entries := make([]*RPCLogEntry, len(request.Entries))
	for i, entry := range request.Entries {
		entries[i] = &RPCLogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Type:    entry.Type,
			Command: entry.Command,
		}
	}
	grpcAppendEntriesRequest := &RPCAppendEntriesRequest{
		Term:         request.Term,
		LeaderId:     request.LeaderId,
		PrevLogIndex: request.PrevLogIndex,
		PrevLogTerm:  request.PrevLogTerm,
		LeaderCommit: request.LeaderCommit,
		Entries:      entries,
	}
	client, err := server.newGRPCClient(peer)
	if err != nil {
		return rft.AppendEntriesResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	grpcAppendEntriesResponse, err := client.AppendEntries(ctx, grpcAppendEntriesRequest)
	if err != nil {
		return rft.AppendEntriesResponse{}, err
	}
	raftAppendEntriesResponse := rft.AppendEntriesResponse{
		Term:    grpcAppendEntriesResponse.Term,
		Success: grpcAppendEntriesResponse.Success,
	}
	return raftAppendEntriesResponse, nil
}

func (server *RaftRpcImplem) InstallSnapshotRPC(peer rft.Peer, req rft.InstallSnapshotRequest) (rft.InstallSnapshotResponse, error) {
	grpcInstallSnapshotRequest := &RPCInstallSnapshotRequest{
		Term:              req.Term,
		LeaderId:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	}
	client, err := server.newGRPCClient(peer)
	if err != nil {
		return rft.InstallSnapshotResponse{}, err
	}
	grpcInstallSnapshotResponse, err := client.InstallSnapshot(context.Background(), grpcInstallSnapshotRequest)
	if err != nil {
		return rft.InstallSnapshotResponse{}, err
	}
	raftInstallSnapshotResponse := rft.InstallSnapshotResponse{
		Term: grpcInstallSnapshotResponse.Term,
		Success: grpcInstallSnapshotResponse.Success,
	}
	return raftInstallSnapshotResponse, nil
}

func (server *RaftRpcImplem) AddClientRPC() (*rft.ClusterInfo, error) {
	return &rft.ClusterInfo{}, nil
}

func (server *RaftRpcImplem) ClientWriteRPC(peer rft.Peer, req rft.ClientRequest) (rft.ClientRequestResponse, error) {
	return rft.ClientRequestResponse{}, nil
}

func (server *RaftRpcImplem) ClientReadRPC(peer rft.Peer, req rft.ClientRequest) (rft.ClientRequestResponse, error) {
	return rft.ClientRequestResponse{}, nil
}

func (server *RaftRpcImplem) ForwardToLeaderRPC(peer rft.Peer, req rft.ClientRequest) (rft.ClientRequestResponse, error) {
	return rft.ClientRequestResponse{}, nil
}

func (server *RaftRpcImplem) JoinClusterRPC(peer rft.Peer, req rft.JoinClusterRequest) (rft.JoinClusterResponse, error) {
	grpcJoinRequest := &RPCJoinClusterRequest{
		Id:   req.Id,
		Addr: req.Addr,
		Port: req.Port,
	}
	client, err := server.newGRPCClient(peer)
	if err != nil {
		return rft.JoinClusterResponse{}, err
	}
	grpcJoinResponse, err := client.JoinCluster(context.Background(), grpcJoinRequest)
	if err != nil {
		return rft.JoinClusterResponse{}, err
	}
	raftJoinResponse := rft.JoinClusterResponse{
		Success: grpcJoinResponse.Success,
		Message: grpcJoinResponse.Message,
	}
	return raftJoinResponse, nil
}

// gRPC raft server implementation
func (server *RaftRpcImplem) RequestVote(ctx context.Context, ballot *RPCBallot) (*RPCBallotResponse, error) {
	raftBallot := rft.Ballot{
		Term:         ballot.Term,
		CandidateId:  ballot.CandidateId,
		LastLogIndex: ballot.LastLogIndex,
		LastLogTerm:  ballot.LastLogTerm,
	}
	raftBallotResponse := server.node.RecvVoteRequest(raftBallot)
	grpcBallotResponse := &RPCBallotResponse{
		Term:        raftBallotResponse.Term,
		VoteGranted: raftBallotResponse.VoteGranted,
	}

	return grpcBallotResponse, nil
}

func (server *RaftRpcImplem) AppendEntries(context context.Context, request *RPCAppendEntriesRequest) (*RPCAppendEntriesResponse, error) {
	entries := make([]rft.LogEntry, len(request.Entries))
	for i, entry := range request.Entries {
		entries[i] = rft.LogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Type:    entry.Type,
			Command: entry.Command,
		}
	}
	raftAppendEntriesRequest := rft.AppendEntriesRequest{
		Term:         request.Term,
		LeaderId:     request.LeaderId,
		PrevLogIndex: request.PrevLogIndex,
		PrevLogTerm:  request.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: request.LeaderCommit,
	}

	raftAppendEntriesResponse := server.node.RecvAppendEntries(raftAppendEntriesRequest)
	grpcAppendEntriesResponse := &RPCAppendEntriesResponse{
		Term:    raftAppendEntriesResponse.Term,
		Success: raftAppendEntriesResponse.Success,
	}
	return grpcAppendEntriesResponse, nil
}

func (server *RaftRpcImplem) JoinCluster(ctx context.Context, request *RPCJoinClusterRequest) (*RPCJoinClusterResponse, error) {
	raftJoinClusterRequest := rft.JoinClusterRequest{
		Id:   request.Id,
		Addr: request.Addr,
		Port: request.Port,
	}
	raftJoinClusterResponse := server.node.RecvJoinClusterRequest(raftJoinClusterRequest)
	grpcJoinClusterResponse := &RPCJoinClusterResponse{
		Success: raftJoinClusterResponse.Success,
		Message: raftJoinClusterResponse.Message,
	}
	return grpcJoinClusterResponse, nil
}

func (server *RaftRpcImplem) InstallSnapshot(ctx context.Context, request *RPCInstallSnapshotRequest) (*RPCInstallSnapshotResponse, error) {
	raftInstallSnapshotRequest := rft.InstallSnapshotRequest{
		Term:              request.Term,
		LeaderId:          request.LeaderId,
		LastIncludedIndex: request.LastIncludedIndex,
		LastIncludedTerm:  request.LastIncludedTerm,
		Data:              request.Data,
	}
	raftInstallSnapshotResponse := server.node.RecvInstallSnapshotRequest(raftInstallSnapshotRequest)
	grpcInstallSnapshotResponse := &RPCInstallSnapshotResponse{
		Term: raftInstallSnapshotResponse.Term,
		Success: raftInstallSnapshotResponse.Success,
	}
	return grpcInstallSnapshotResponse, nil
}

// gRPC raft client service implementation

func (server *RaftRpcImplem) AddClient(ctx context.Context, e *emptypb.Empty) (*ClusterInfo, error) {
	info := server.node.HandleClientHello()
	return &ClusterInfo{IsLeader: info.IsLeader}, nil
}
func (server *RaftRpcImplem) Write(ctx context.Context, request *WriteRequest) (*WriteResponse, error) {
	var command string = request.Command
	var raftRequest rft.ClientRequest = rft.ClientRequest{
		Command: []byte(command),
	}
	var raftResponse rft.ClientRequestResponse = server.node.RecvClientRequest(raftRequest)
	response := &WriteResponse{
		Success: raftResponse.Success,
	}
	return response, nil
}

func (server *RaftRpcImplem) Read(ctx context.Context, request *ReadRequest) (*ReadResponse, error) {
	var command string = request.Command
	var raftRequest rft.ClientReadRequest = rft.ClientReadRequest{
		Command: []byte(command),
	}
	var raftResponse rft.ClientReadResponse = server.node.RecvClientReadRequest(raftRequest)
	response := &ReadResponse{
		Response: string(raftResponse.Response),
		Success:  raftResponse.Success,
		Error:    raftResponse.Error,
	}
	return response, nil
}
