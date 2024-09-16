package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	raft "raft.com/raft"
	rpc "raft.com/raft/grpc"
)

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

	rpc := &rpc.RaftRpcImplem{}

	sm := raft.DebugStateMachine{}
	node := raft.NewNode(
		uint64(id),
		addr,
		rpc,
		&sm,
		"./conf",
	)
	node.Peers = peers
	node.Start()
}
