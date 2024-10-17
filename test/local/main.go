package main

import (
	"fmt"
	"sync"
	"time"

	"raft.com/raft"
	rpc "raft.com/raft/grpc"
	drop "raft.com/simulate/rpc"
)

var nbCommands = 50
var dropRate float32 = 0.95

func NewDropRPC(dropRate float32) raft.RaftRPC {
	baseRpc := &rpc.RaftRpcImplem{}
	return drop.NewDropRPC(dropRate, baseRpc)
}

func main() {
	config := raft.NodeConfig{
		ElectionTimeoutMin: 500,
		ElectionTimeoutMax: 1500,
		HeartbeatTimeout:   50,
	}
	node1 := raft.NewDebugNode(1, "localhost:9001", NewDropRPC(dropRate), config)
	node2 := raft.NewDebugNode(2, "localhost:9002", NewDropRPC(dropRate), config)
	node3 := raft.NewDebugNode(3, "localhost:9003", NewDropRPC(dropRate), config)

	nodes := []*raft.DebugNode{node1, node2, node3}

	node1.Node.Peers = []raft.Peer{{Id: 2, Addr: "localhost:9002"}, {Id: 3, Addr: "localhost:9003"}}
	node2.Node.Peers = []raft.Peer{{Id: 1, Addr: "localhost:9001"}, {Id: 3, Addr: "localhost:9003"}}
	node3.Node.Peers = []raft.Peer{{Id: 1, Addr: "localhost:9001"}, {Id: 2, Addr: "localhost:9002"}}

	var wg sync.WaitGroup
	wg.Add(3)

	for _, node := range nodes {
		go func(n *raft.DebugNode) {
			n.Start()
		}(node)
	}
	time.Sleep(3 * time.Second)

	client := rpc.NewRaftGrpcClient()
	connectErr := client.ConnectToCluster([]string{"localhost:9001", "localhost:9002", "localhost:9003"})

	if connectErr != nil {
		fmt.Println("Failed to connect to cluster")
	}

	for i := 0; i < nbCommands; i++ {
		response := client.Write(fmt.Sprintf("command %d", i))
		if !response.Success {
			fmt.Println("Failed to write command, retrying to connect to cluster")
			connectErr = client.ConnectToCluster([]string{"localhost:9001", "localhost:9002", "localhost:9003"})
			if connectErr != nil {
				fmt.Println("Failed to connect to cluster")
			}
			time.Sleep(1 * time.Second)
		}
		time.Sleep(100 * time.Millisecond)
	}
	dumpedLogs := [][]raft.LogEntry{}
	for i, node := range nodes {
		dump := node.Stop()
		dumpedLogs = append(dumpedLogs, dump)
		fmt.Println("Node ", i, " logs: ", len(dump))
	}

	minLen := len(dumpedLogs[0])
	for i := 1; i < len(dumpedLogs); i++ {
		if len(dumpedLogs[i]) != minLen {
			fmt.Println("Logs are not the same length")
		}
		if len(dumpedLogs[i]) < minLen {
			minLen = len(dumpedLogs[i])
		}
	}
	for i := 0; i < minLen; i++ {
		for j := 0; j < len(dumpedLogs); j++ {
			if string(dumpedLogs[j][i].Command) != string(dumpedLogs[0][i].Command) {
				fmt.Println("Logs are not the same")
			}
		}
	}

}
