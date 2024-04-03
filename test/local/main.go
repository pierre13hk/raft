package main

import (
	"sync"
	"time"
	"raft.com/raft"
)

func main() {
	raft.NewNode(1)
	
	node1 := raft.NewNode(1)
	node2 := raft.NewNode(2)
	node3 := raft.NewNode(3)
	node4 := raft.NewNode(4)
	node5 := raft.NewNode(5)
	//nodes := []*raft.Node{node1, node2, node3}
	nodes := []*raft.Node{node1, node2, node3, node4, node5}

	node1.Peers = []raft.Peer{{Id: 2}, {Id: 3}}
	node2.Peers = []raft.Peer{{Id: 1}, {Id: 3}}
	node3.Peers = []raft.Peer{{Id: 1}, {Id: 2}}

	node1.Peers = []raft.Peer{{Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}}
	node2.Peers = []raft.Peer{{Id: 1}, {Id: 3}, {Id: 4}, {Id: 5}}
	node3.Peers = []raft.Peer{{Id: 1}, {Id: 2}, {Id: 4}, {Id: 5}}
	node4.Peers = []raft.Peer{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 5}}
	node5.Peers = []raft.Peer{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}}

	
	rpc := raft.NewInMemoryRaftRPC()
	rpc.Peers = map[uint64]*raft.Node{
		1: node1,
		2: node2,
		3: node3,
		4: node4,
		5: node5,
	}

	var wg sync.WaitGroup
	wg.Add(5)
	for _, node := range nodes {
		node.RaftRPC = rpc
		
	}
	
	for _, node := range nodes {
		go func(n *raft.Node) {
			n.Start()
		}(node)
	}
	
	for {
		time.Sleep(1 * time.Second)
	}
	wg.Wait()
}