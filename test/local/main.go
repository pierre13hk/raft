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
	
	time.Sleep(100 * time.Second)
	lst := make([][]raft.LogEntry, 0)
	for _, node := range nodes {
		lst = append(lst, node.Stop())
		wg.Done()
	}
	
	
	for i, logs := range lst {
		fmt.Println("Log entries: for node ", i)
		for _, log := range logs {
			fmt.Println(string(log.Command))
		}
	}
	

	wg.Wait()
}