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
var dropRate float32 = 0.5

func NewDropRPC(dropRate float32) raft.RaftRPC {
	baseRpc := &rpc.RaftRpcImplem{}
	return drop.NewDropRPC(dropRate, baseRpc)
}

func main() {

	node1 := raft.NewDebugNode(1, "localhost:9001", NewDropRPC(dropRate))
	node2 := raft.NewDebugNode(2, "localhost:9002", NewDropRPC(dropRate))
	node3 := raft.NewDebugNode(3, "localhost:9003", NewDropRPC(dropRate))

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
		fmt.Println("Error connecting to cluster")
	}

	for i := 0; i < nbCommands; i++ {
		fmt.Println("Writing command")
		cmd := fmt.Sprintf("b %d f", i)
		resp := client.Write(cmd)
		if !resp.Success {
			fmt.Println("Error writing command")
			client.ConnectToCluster([]string{"localhost:9001", "localhost:9002", "localhost:9003"})
			continue
		}
		fmt.Println("Command written")
	}

	lst := make([][]raft.LogEntry, 0)
	for _, node := range nodes {
		lst = append(lst, node.Stop())
		wg.Done()
	}

	for i, logs := range lst {
		if len(logs) != len(lst[0]) {
			fmt.Printf("Node %d has different log length\n", i+1)
		}
		for j, log := range logs {
			if log.Term != lst[0][j].Term || log.Index != lst[0][j].Index {
				fmt.Printf("Node %d has different log at index %d\n", i+1, j+1)
			}
		}
		fmt.Printf("Node %d has %d logs\n", i+1, len(logs))
	}
	for i, _ := range lst[0] {
		fmt.Printf("Log at index %d: %s | ", lst[0][i].Index, string(lst[0][i].Command))
		fmt.Printf("Log at index %d: %s | ", lst[1][i].Index, string(lst[1][i].Command))
		fmt.Printf("Log at index %d: %s | \n", lst[2][i].Index, string(lst[2][i].Command))
	}
	wg.Wait()
}
