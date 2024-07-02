package raft

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

type Logger interface {
	// Truncate the log from the given index to the end
	TruncateFrom(index uint64)
	// Truncate the log from the beginning to the given index (exclusive)
	TruncateTo(index uint64)
	// Get the term of the last log entry
	LastLogTerm() uint64
	// Get the index of the last log entry
	LastLogIndex() uint64
	// Get the log entry at the given index
	Get(index uint64) (LogEntry, error)
	// Get all log entries starting from the given index
	GetFrom(start uint64) ([]LogEntry, error)
	// Get all log entries with index in the range [start, end)
	GetRange(start uint64, end uint64) ([]LogEntry, error)
	// Append log entries to the log
	Append(entries []LogEntry) error
}
