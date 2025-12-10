package rpc

/*
 * A simple test rcp implementation that drops / fails RPCs at a given rate
 */

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/pierre13hk/raft/raft"
)

var ErrPartitionRPC = errors.New("node is partitioned")

type PartitionRPC struct {
	intervalPeriodMs int32
	outageMs         int32
	transmitting     bool
	timer            *time.Timer
	running          bool
	raftRpcImpl      raft.RaftRPC
}

func NewPartitionRPC(intervalMs, outageMs int32, rpc raft.RaftRPC) *PartitionRPC {
	return &PartitionRPC{
		intervalPeriodMs: intervalMs,
		outageMs:         outageMs,
		transmitting:     true,
		timer:            time.NewTimer(100 * time.Second),
		raftRpcImpl:      rpc,
	}
}

func (d *PartitionRPC) scheduleNextOutage() {
	timeToNextOutage := rand.Int31() % d.intervalPeriodMs
	d.timer.Reset(time.Duration(timeToNextOutage) * time.Millisecond)
}

func (d *PartitionRPC) scheduleNextTransmission() {
	timeToNextTransmission := rand.Int31() % d.outageMs
	d.timer.Reset(time.Duration(timeToNextTransmission) * time.Millisecond)
}

func (d *PartitionRPC) timerHandler() {
	for d.running {
		<-d.timer.C
		if d.transmitting {
			d.transmitting = false
			d.raftRpcImpl.Stop()
			d.scheduleNextTransmission()
		} else {
			d.transmitting = true
			d.raftRpcImpl.Start()
			d.scheduleNextOutage()

		}
	}
}

func (d *PartitionRPC) RegisterNode(node *raft.Node) {
	d.raftRpcImpl.RegisterNode(node)
	fmt.Println("PartitionRPC Registered Node")
}

func (d *PartitionRPC) Start() {
	d.running = true
	d.scheduleNextOutage()
	go d.timerHandler()
	d.raftRpcImpl.Start()
}

func (d *PartitionRPC) Stop() {
	d.running = false
	d.raftRpcImpl.Stop()
}

func (d *PartitionRPC) RequestVoteRPC(peer raft.Peer, ballot raft.Ballot) (raft.BallotResponse, error) {
	if !d.transmitting {
		return raft.BallotResponse{}, ErrPartitionRPC
	}
	return d.raftRpcImpl.RequestVoteRPC(peer, ballot)
}

func (d *PartitionRPC) AppendEntriesRPC(peer raft.Peer, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if !d.transmitting {
		return raft.AppendEntriesResponse{}, ErrPartitionRPC
	}
	return d.raftRpcImpl.AppendEntriesRPC(peer, req)
}

func (d *PartitionRPC) ForwardToLeaderRPC(peer raft.Peer, req raft.ClientRequest) (raft.ClientRequestResponse, error) {
	if !d.transmitting {
		return raft.ClientRequestResponse{}, ErrPartitionRPC
	}
	return d.raftRpcImpl.ForwardToLeaderRPC(peer, req)
}

func (d *PartitionRPC) JoinClusterRPC(peer raft.Peer, req raft.JoinClusterRequest) (raft.JoinClusterResponse, error) {
	if !d.transmitting {
		return raft.JoinClusterResponse{}, ErrPartitionRPC
	}
	return d.raftRpcImpl.JoinClusterRPC(peer, req)
}

func (d *PartitionRPC) AddClientRPC() (*raft.ClusterInfo, error) {
	return d.raftRpcImpl.AddClientRPC()
}

func (d *PartitionRPC) ClientWriteRPC(peer raft.Peer, req raft.ClientRequest) (raft.ClientRequestResponse, error) {
	if !d.transmitting {
		return raft.ClientRequestResponse{}, ErrPartitionRPC
	}
	return d.raftRpcImpl.ClientWriteRPC(peer, req)
}

func (d *PartitionRPC) ClientReadRPC(peer raft.Peer, req raft.ClientRequest) (raft.ClientRequestResponse, error) {
	if !d.transmitting {
		return raft.ClientRequestResponse{}, ErrPartitionRPC
	}
	return d.raftRpcImpl.ClientReadRPC(peer, req)
}

func (d *PartitionRPC) InstallSnapshotRPC(peer raft.Peer, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	if !d.transmitting {
		return raft.InstallSnapshotResponse{}, ErrPartitionRPC
	}
	return d.raftRpcImpl.InstallSnapshotRPC(peer, req)
}
