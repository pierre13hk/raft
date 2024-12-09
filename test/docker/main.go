package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	raft "raft.com/raft"
	rpc "raft.com/raft/grpc"
	drop "raft.com/simulate/rpc"
)

func client(peers []raft.Peer) {
	time.Sleep(3 * time.Second)
	clusterAddresses := make([]string, len(peers))
	for i, p := range peers {
		clusterAddresses[i] = p.Addr
	}
	client := rpc.NewRaftGrpcClient()
	connectErr := client.ConnectToCluster(clusterAddresses)
	if connectErr != nil {
		fmt.Println("Failed to connect to cluster")
	}

	requests, _ := strconv.Atoi(os.Getenv("REQUESTS"))
	sleepTime, _ := strconv.Atoi(os.Getenv("WAIT_BETWEEN_REQUEST_MS"))
	for i := 0; i < requests; i++ {
		response := client.Write(fmt.Sprintf("command %d", i))
		if !response.Success {
			fmt.Println("Failed to write command, retrying to connect to cluster")
			connectErr = client.ConnectToCluster(clusterAddresses)
			if connectErr != nil {
				fmt.Println("Failed to connect to cluster")
			}
		}
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}

func dropRPC(dropRate float32) raft.RaftRPC {
	baseRpc := &rpc.RaftRpcImplem{}
	return drop.NewDropRPC(dropRate, baseRpc)
}

func main() {
	id, _ := strconv.ParseInt(os.Getenv("ID"), 10, 64)
	port := os.Getenv("PORT")
	addr := fmt.Sprintf("0.0.0.0:%s", port)
	peersStr := os.Getenv("PEERS")
	peersLst := strings.Split(peersStr, ",")
	peers := make([]raft.Peer, len(peersLst))
	for i, p := range peersLst {
		idIp := strings.Split(p, "|")
		id, _ := strconv.ParseInt(idIp[0], 10, 64)
		ip := idIp[1]
		peers[i] = raft.Peer{Id: uint64(id), Addr: ip}
	}

	isClient := os.Getenv("CLIENT")
	if isClient == "true" {
		client(peers)
		return
	} else {
		dropRateStr := os.Getenv("DROP_RATE")
		dropRate := float32(0)
		if len(dropRateStr) == 0 {
			dropRate = 0
		} else {
			dropRate64, _ := strconv.ParseFloat(dropRateStr, 32)
			dropRate = float32(dropRate64)
			rpc := dropRPC(dropRate)
			sm := raft.DebugStateMachine{}
			conf := raft.NodeConfig{
				ElectionTimeoutMin: 600,
				ElectionTimeoutMax: 1000,
				HeartbeatTimeout:   400,
			}
			node := raft.NewNode(
				uint64(id),
				addr,
				rpc,
				&sm,
				"./app/conf",
				conf,
			)
			node.Peers = peers
			node.Start()
			wg := sync.WaitGroup{}
			wg.Add(1)
			wg.Wait()
		}
	}

}
