package raft

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"
)

type Snapshot struct{}

type SnapshotInfo struct {
	LastCommitedIndex uint64
	Date              string
}

func (l *Node) getLastSnapsotFileName() (string, error) {
	entries, err := os.ReadDir(SNAPSHOT_DIR)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "nil", errors.New("No snapshot found")
	}
	var latestSnapshot os.DirEntry
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if latestSnapshot == nil {
			latestSnapshot = entry
			continue
		}
		info, err := entry.Info()
		if err == nil {
			latestInfo, _ := latestSnapshot.Info()
			if info.ModTime().After(latestInfo.ModTime()) {
				latestSnapshot = entry
			}
		}
	}
	fullPath := fmt.Sprintf("%s/%s", SNAPSHOT_DIR, latestSnapshot.Name())
	return fullPath, nil
}

func (n *Node) createSnapshotsDir() error {
	err := os.MkdirAll(SNAPSHOT_DIR, 0777)
	if err != nil {
		log.Fatalf("Error creating conf dir: " + err.Error() + "snapshot dir: " + SNAPSHOT_DIR)
		return err
	}
	return nil
}

func (n *Node) CreateSnapshot(sm StateMachine, lastCommitedIndex uint64) error {
	snapshotCount := len(n.snapshotsInfo)
	snapshotFileName := fmt.Sprintf("%s/%d%s", SNAPSHOT_DIR, snapshotCount, spashotSuffix)
	bytes, err := sm.Serialize()
	if err != nil {
		log.Println("Error serializing state machine")
		return err
	}
	err = n.saveSnapshotToDisk(bytes)
	if err != nil {
		log.Println("Error saving snapshot to disk")
		return err
	}
	snapshotInfo := SnapshotInfo{LastCommitedIndex: lastCommitedIndex, Date: "now"}
	n.snapshotsInfo[snapshotFileName] = snapshotInfo
	n.lastSnapshotName = snapshotFileName
	err = n.saveConfig()
	//err = n.state.Cut(lastCommitedIndex)
	if err != nil {
		log.Fatalf("Error truncating log: " + err.Error())
		return err
	}
	log.Println("Snapshot created, truncated to: " + fmt.Sprint(lastCommitedIndex))
	return nil
}

func (n *Node) saveSnapshotToDisk(snapshot []byte) error {
	snapshotFileName := fmt.Sprintf("%s/%d%s", SNAPSHOT_DIR, len(n.snapshotsInfo), spashotSuffix)
	snapshotFile, err := os.Create(snapshotFileName)
	if err != nil {
		return err
	}
	defer snapshotFile.Close()
	_, err = snapshotFile.Write(snapshot)
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) GetLastSnapshot() ([]byte, error) {
	snapshotFileName, err := n.getLastSnapsotFileName()
	if err != nil {
		return nil, err
	}
	file, err := os.Open(snapshotFileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, info.Size())
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (n *Node) RecoverFromSnapshot() error {
	return nil
}

func (n *Node) installSnapshot(snapshot *Snapshot) error {
	return nil
}

func (n *Node) installSnapshotOnNewPeer(peer Peer) {
	/* Install a snapshot on a new peer */
	commitIndex := n.state.commitIndex
	log.Println("Node ", n.state.id, " creating snapshot for new peer ", peer.Id)
	err := n.CreateSnapshot(n.StateMachine, commitIndex)
	if err != nil {
		log.Println("Error creating snapshot", err)
		return
	}
	snapshotBytes, err := n.GetLastSnapshot()
	if err != nil {
		log.Println("Error getting snapshot: ", err)
		return
	}
	snapshotRequest := InstallSnapshotRequest{
		Term:              n.state.currentTerm,
		LeaderId:          n.state.id,
		LastIncludedIndex: commitIndex,
		LastIncludedTerm:  n.state.currentTerm,
		LastConfig:        n.Peers,
		Data:              snapshotBytes,
	}
	log.Println("Node ", n.state.id, " installing snapshot on new peer grpc addr", peer.Id, n.RaftRPC)
	response, err := n.RaftRPC.InstallSnapshotRPC(peer, snapshotRequest)
	log.Println("Node ", n.state.id, " installed snapshot on new peer ", peer.Id, " response ", response, " error ", err)
	for err != nil {
		log.Println("Error installing snapshot on new peer", err)
		response, err = n.RaftRPC.InstallSnapshotRPC(peer, snapshotRequest)
		time.Sleep(100 * time.Millisecond)
	}
	if response.Success {
		log.Println("Node ", n.state.id, " installed snapshot on new peer ", peer.Id)
	} else {
		log.Println("Node ", n.state.id, " failed to install snapshot on new peer ", peer.Id)
	}
}
