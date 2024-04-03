package raft

import (
	"log"
	"time"
)

type FollowerReplicationState struct {
	nextIndex  uint64
	matchIndex uint64
}

func (n *Node) leaderHeartbeat() {
	/* Send AppendEntries RPC to all peers */
	request := AppendEntriesRequest{
		Term:         n.state.currentTerm,
		LeaderId:     n.state.id,
		PrevLogIndex: n.state.logger.LastLogIndex(),
		PrevLogTerm:  n.state.logger.LastLogTerm(),
		Entries:      []LogEntry{},
		LeaderCommit: n.state.commitIndex,
	}

	for _, peer := range n.Peers {
		if peer.Id == n.state.id {
			continue
		}
		go func(p Peer) {
			n.RaftRPC.AppendEntriesRPC(p, request)
		}(peer)
	}

	n.RestartHeartbeatTimerLeader()
}

func (n *Node) RestartHeartbeatTimerLeader() {
	/* Restart the heartbeat timer */
	n.timer.Stop()
	n.timer.Reset(time.Duration(500) * time.Millisecond)
}

func (n *Node) leaderDaemon() {
	/* Leader daemon */
	n.timer.Stop()
	for i := 0; i < 5; i += 1 {
		n.leaderHeartbeat()
		time.Sleep(500 * time.Millisecond)
	}
	log.Printf("Node %d : Leader daemon stopped\n", n.state.id)
	n.state.votedFor = 0
	n.role = Follower
	n.RestartElectionTimer()
}
