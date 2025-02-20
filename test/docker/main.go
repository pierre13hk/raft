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

func runNodeAfterClient(peers []raft.Peer) {
	fmt.Println("Running node after client")
	rpc := getRPC()
	sm := NewMapSM()
	conf := raft.NodeConfig{
		ElectionTimeoutMin: 600,
		ElectionTimeoutMax: 1000,
		HeartbeatTimeout:   100,
	}
	addr := fmt.Sprintf("0.0.0.0:%d", 9004)
	node := raft.NewNode(
		uint64(4),
		addr,
		rpc,
		sm,
		"/app/conf",
		conf,
	)
	node.Peers = peers
	peerAddrs := make([]string, len(peers))
	for i, p := range peers {
		peerAddrs[i] = p.Addr
	}
	joined := node.BootstrapCluster("client", "9004", peerAddrs...)
	for !joined {
		joined = node.BootstrapCluster("client", "9004", peerAddrs...)
		time.Sleep(500 * time.Millisecond)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()

}

func client(peers []raft.Peer) {
	time.Sleep(3 * time.Second)
	clusterAddresses := make([]string, len(peers))
	for i, p := range peers {
		clusterAddresses[i] = p.Addr
	}
	var clientStub raft.RaftClientStub = rpc.NewRaftGrpcClient()
	client := raft.NewRaftClient(clientStub, peers...)

	requests, _ := strconv.Atoi(os.Getenv("REQUESTS"))
	sleepTime, _ := strconv.Atoi(os.Getenv("WAIT_BETWEEN_REQUEST_MS"))
	for i := 0; i < requests; i++ {
		command := fmt.Sprintf("set|%d|%d", i, i)
		response, err := client.Write(command)
		if err != nil || !response.Success {
			fmt.Println("Failed to write command, retrying to connect to cluster")
		}
		fmt.Println("Successfully wrote command ", response.Success)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
	for i := 0; i < requests; i++ {
		command := fmt.Sprintf("%d", i)
		response, err := client.Read(raft.ClientReadRequest{Command: []byte(command)})
		if err != nil {
			fmt.Println("Failed to read command")
			continue
		}
		if response.Success {
			fmt.Println("Read response: ", string(response.Response))
		} else {
			fmt.Println("The state machine returned an error: ", response.Error)
		}
	}

	runNodeAfterClient(peers)
}

func dropRPC(dropRate float32, rpc raft.RaftRPC) raft.RaftRPC {
	return drop.NewDropRPC(dropRate, rpc)
}

func partitionRPC(intervalMs, outageMs int32, rpc raft.RaftRPC) raft.RaftRPC {
	return drop.NewPartitionRPC(intervalMs, outageMs, rpc)
}

func getRPC() raft.RaftRPC {
	var rpc raft.RaftRPC = &rpc.RaftRpcImplem{}
	usePartition := os.Getenv("PARTITION") == "true"
	if usePartition {
		intervalMs, _ := strconv.Atoi(os.Getenv("PARTITION_INTERVAL_MS"))
		outageMs, _ := strconv.Atoi(os.Getenv("PARTITION_OUTAGE_MS"))
		rpc = partitionRPC(int32(intervalMs), int32(outageMs), rpc)
	}

	useDrop := os.Getenv("DROP_RATE") != ""
	if useDrop {
		dropRateStr := os.Getenv("DROP_RATE")
		var dropRate float32 = 0.0
		if len(dropRateStr) != 0 {
			dropRate64, _ := strconv.ParseFloat(dropRateStr, 32)
			dropRate = float32(dropRate64)
		}
		rpc = dropRPC(dropRate, rpc)
		fmt.Println("Using drop rate", dropRate)
	}
	return rpc
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

		rpc := getRPC()
		sm := NewMapSM()
		conf := raft.NodeConfig{
			ElectionTimeoutMin: 600,
			ElectionTimeoutMax: 1000,
			HeartbeatTimeout:   400,
		}
		node := raft.NewNode(
			uint64(id),
			addr,
			rpc,
			sm,
			"/app/raft/conf",
			conf,
		)
		node.Peers = peers
		node.Start()
		wg := sync.WaitGroup{}
		wg.Add(1)
		wg.Wait()

	}

}
