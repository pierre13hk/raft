package raft

import (
	"testing"
)

func TestTruncateStart(t *testing.T) {
	logger := &InMemoryLogger{
		entries: []LogEntry{
			{Term: 1, Index: 0, Command: []byte("a")},
			{Term: 1, Index: 1, Command: []byte("b")},
			{Term: 1, Index: 2, Command: []byte("c")},
		},
	}

	logger.TruncateFrom(1)

	if len(logger.entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(logger.entries))
	}

	if logger.entries[0].Index != 1 {
		t.Fatalf("expected first entry to be index 2, got %d", logger.entries[0].Index)
	}
}

func TestTruncateEnd(t *testing.T) {
	logger := &InMemoryLogger{
		entries: []LogEntry{
			{Term: 1, Index: 0, Command: []byte("a")},
			{Term: 1, Index: 1, Command: []byte("b")},
			{Term: 1, Index: 2, Command: []byte("c")},
		},
	}

	logger.TruncateTo(2)

	if len(logger.entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(logger.entries))
	}

	if logger.entries[1].Index != 1 {
		t.Fatalf("expected second entry to be index 1, got %d", logger.entries[0].Index)
	}

	logger.TruncateTo(1)

	if len(logger.entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(logger.entries))
	}

	if logger.entries[0].Index != 0 {
		t.Fatalf("expected first entry to be index 1, got %d", logger.entries[0].Index)
	}
}
