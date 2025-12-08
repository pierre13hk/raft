package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	raft "raft.com/raft"
	rpc "raft.com/raft/grpc"
	drop "raft.com/simulate/rpc"
)

func printUsage() {
	fmt.Println("Usage: client write <key> <value> or client read <key>")
}

func runClient(peers []raft.Peer) {
	args := os.Args
	if len(args) < 3 || (args[1] != "write" && args[1] != "read") {
		fmt.Println("missing args or wrong cmd", args)
		printUsage()
		return
	}

	clusterAddresses := make([]string, len(peers))
	for i, p := range peers {
		clusterAddresses[i] = p.Addr
	}
	var clientStub raft.RaftClientStub = rpc.NewRaftGrpcClient()
	client := raft.NewRaftClient(clientStub, peers...)

	if args[1] == "read" {
		response, err := client.Read(raft.ClientReadRequest{Command: []byte(args[2])})
		if err != nil {
			fmt.Println("Error reading:", err)
		} else {
			fmt.Println("Response contents", string(response.Response), "Response error", response.Error)
		}
	} else {
		if len(args) != 4 {
			fmt.Println("missings args for write", args)
			printUsage()
			return
		}
		_, err := client.Write(fmt.Sprintf("set|%s|%s", args[2], args[3]))
		if err != nil {
			fmt.Println("Error writing:", err)
		}
	}
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
		runClient(peers)
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
