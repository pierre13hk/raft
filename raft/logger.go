package raft

import ()

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

type Logger interface {
	TruncateFrom(index uint64)
	TruncateTo(index uint64)
	LastLogTerm() uint64
	LastLogIndex() uint64
	Get(index uint64) (LogEntry, error)
	GetRange(start uint64) ([]LogEntry, error)
	Append(entries []LogEntry) error
}
