package rpc

/*
 * A simple test rcp implementation that drops / fails RPCs at a given rate
 */

import (
	"errors"
	"fmt"
	"math/rand"

	"raft.com/raft"
)

var ErrDropRPC = errors.New("RPC dropped")

type DropRPC struct {
	dropRate    float32
	raftRpcImpl raft.RaftRPC
}

func NewDropRPC(dropRate float32, raftRpcImpl raft.RaftRPC) DropRPC {
	return DropRPC{dropRate: dropRate, raftRpcImpl: raftRpcImpl}
}

func (d DropRPC) RegisterNode(node *raft.Node) {
	d.raftRpcImpl.RegisterNode(node)
}

func (d DropRPC) Start() {
	d.raftRpcImpl.Start()
}

func (d DropRPC) RequestVoteRPC(peer raft.Peer, ballot raft.Ballot) (raft.BallotResponse, error) {
	if rand.Float32() < d.dropRate {
		fmt.Println("Dropping RequestVoteRPC")
		return raft.BallotResponse{}, ErrDropRPC
	}
	return d.raftRpcImpl.RequestVoteRPC(peer, ballot)
}

func (d DropRPC) AppendEntriesRPC(peer raft.Peer, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if rand.Float32() < d.dropRate {
		fmt.Println("Dropping AppendEntriesRPC")
		return raft.AppendEntriesResponse{}, ErrDropRPC
	}
	return d.raftRpcImpl.AppendEntriesRPC(peer, req)
}

func (d DropRPC) ForwardToLeaderRPC(peer raft.Peer, req raft.ClientRequest) (raft.ClientRequestResponse, error) {
	if rand.Float32() < d.dropRate {
		return raft.ClientRequestResponse{}, ErrDropRPC
	}
	return d.raftRpcImpl.ForwardToLeaderRPC(peer, req)
}

func (d DropRPC) JoinClusterRPC(peer raft.Peer, req raft.JoinClusterRequest) (raft.JoinClusterResponse, error) {
	if rand.Float32() < d.dropRate {
		return raft.JoinClusterResponse{}, ErrDropRPC
	}
	return d.raftRpcImpl.JoinClusterRPC(peer, req)
}

func (d DropRPC) AddClientRPC() (*raft.ClusterInfo, error) {
	return d.raftRpcImpl.AddClientRPC()
}

func (d DropRPC) ClientWriteRPC(peer raft.Peer, req raft.ClientRequest) (raft.ClientRequestResponse, error) {
	if rand.Float32() < d.dropRate {
		return raft.ClientRequestResponse{}, ErrDropRPC
	}
	return d.raftRpcImpl.ClientWriteRPC(peer, req)
}

func (d DropRPC) InstallSnapshotRPC(peer raft.Peer, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	if rand.Float32() < d.dropRate {
		return raft.InstallSnapshotResponse{}, ErrDropRPC
	}
	return raft.InstallSnapshotResponse{}, nil
}