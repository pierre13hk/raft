package main

import (
	"sync"
	"fmt"
	"time"
	"raft.com/raft"
)

func main() {
	node1 := raft.NewDebugNode(1)
	node2 := raft.NewDebugNode(2)
	node3 := raft.NewDebugNode(3)
	//node4 := raft.NewNode(4)
	//node5 := raft.NewNode(5)
	nodes := []*raft.DebugNode{node1, node2, node3}
	//nodes := []*raft.Node{node1, node2, node3, node4, node5}

	node1.Node.Peers = []raft.Peer{{Id: 2}, {Id: 3}}
	node2.Node.Peers = []raft.Peer{{Id: 1}, {Id: 3}}
	node3.Node.Peers = []raft.Peer{{Id: 1}, {Id: 2}}

	//node1.Peers = []raft.Peer{{Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}}
	//node2.Peers = []raft.Peer{{Id: 1}, {Id: 3}, {Id: 4}, {Id: 5}}
	//node3.Peers = []raft.Peer{{Id: 1}, {Id: 2}, {Id: 4}, {Id: 5}}
	//node4.Peers = []raft.Peer{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 5}}
	//node5.Peers = []raft.Peer{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}}

	
	rpc := raft.NewInMemoryRaftRPC()
	rpc.Peers = map[uint64]*raft.Node{
		1: node1.Node,
		2: node2.Node,
		3: node3.Node,
		//4: node4,
		//5: node5,
	}

	var wg sync.WaitGroup
	wg.Add(3)
	for _, node := range nodes {
		node.Node.RaftRPC = rpc
		
	}
	
	for _, node := range nodes {
		go func(n *raft.DebugNode) {
			n.Start()
		}(node)
	}
	time.Sleep(3 * time.Second)
	client := node1
	for i := 0; i < 13; i++ {
		client.Apply([]byte(fmt.Sprintf("command %d", i)))
		time.Sleep(200 * time.Millisecond)
	}
	client = node2
	for i := 0; i < 13; i++ {
		client.Apply([]byte(fmt.Sprintf("command %d", i)))
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)
	client = node3
	for i := 0; i < 13; i++ {
		client.Apply([]byte(fmt.Sprintf("command %d", i)))
		time.Sleep(200 * time.Millisecond)
	}
	

	time.Sleep(13 * time.Second)
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
	}
	for i,_ := range lst[0] {
		fmt.Printf("Log at index %d: %s | ", lst[0][i].Index, string(lst[0][i].Command))
		fmt.Printf("Log at index %d: %s | ", lst[1][i].Index, string(lst[1][i].Command))
		fmt.Printf("Log at index %d: %s | \n", lst[2][i].Index, string(lst[2][i].Command))
	}

	

	wg.Wait()
}