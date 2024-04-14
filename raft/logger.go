package raft

import (
	"errors"
)

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

type InMemoryLogger struct {
	entries []LogEntry
}

func (l *InMemoryLogger) TruncateFrom(index uint64) {
	l.entries = l.entries[index:]
}

func (l *InMemoryLogger) TruncateTo(index uint64) {
	l.entries = l.entries[:index-1]
}

func (l *InMemoryLogger) LastLogTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

func (l *InMemoryLogger) LastLogIndex() uint64 {
	return l.entries[len(l.entries)-1].Index
}

func (l *InMemoryLogger) Get(index uint64) (LogEntry, error) {
	if uint64(len(l.entries)) < index {
		return LogEntry{}, errors.New("No such entry")
	}
	return l.entries[index-1], nil
}

func (l *InMemoryLogger) GetRange(start uint64) ([]LogEntry, error) {
	if uint64(len(l.entries)) < start {
		return nil, errors.New("No such entry")
	}
	return l.entries[start-1:], nil
}

func (l *InMemoryLogger) Append(entries []LogEntry) error {
	l.entries = append(l.entries, entries...)

	return nil
}
