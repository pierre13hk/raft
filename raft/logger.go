package raft

type LogEntry struct {
	Term uint64
	Index uint64
	Command []byte
}

type Logger interface {
	TruncateStart(index uint64)
	TruncateEnd(index int64)
	AppendEntries(entries []LogEntry)
}

type InMemoryLogger struct {
	entries []LogEntry
}

func (l *InMemoryLogger) TruncateStart(index uint64) {
	l.entries = l.entries[index:]
}

func (l *InMemoryLogger) TruncateEnd(index int64) {
	l.entries = l.entries[:index]
}

func (l *InMemoryLogger) AppendEntries(entries []LogEntry) {
	l.entries = append(l.entries, entries...)
}
