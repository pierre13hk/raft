package raft

import (
	"fmt"
	"os"
	"testing"
)

func TestTruncateStartDebug(t *testing.T) {
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

func TestTruncateEndDebug(t *testing.T) {
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

var tmpLogPath = "/tmp/test.log"

func TestLoggerInitialize(t *testing.T) {
	logger := NewLoggerImplem()
	err := logger.Initialize(tmpLogPath, '\n')
	defer os.Remove(tmpLogPath)
	if err != nil {
		t.Fatalf("expected logger to be initialized")
	}
}

func TestLoggerAppend(t *testing.T) {
	logger := NewLoggerImplem()
	err := logger.Initialize(tmpLogPath, '\n')
	defer os.Remove(tmpLogPath)
	if err != nil {
		t.Fatalf("expected logger to be initialized")
	}

	err = logger.Append([]LogEntry{
		{Term: 1, Index: 0, Command: []byte("a")},
		{Term: 1, Index: 1, Command: []byte("b")},
		{Term: 1, Index: 2, Command: []byte("c")},
	})
	if err != nil {
		fmt.Println(err)
		t.Fatalf("expected entries to be appended")
	}

	logger2 := NewLoggerImplem()
	err = logger2.Initialize(tmpLogPath, '\n')
	if err != nil {
		t.Fatalf("expected second logger to be initialized")
	}

	if len(logger2.inMemEntries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(logger2.inMemEntries))
	}

	for idx, _ := range logger2.inMemEntries {
		wrote := logger.inMemEntries[idx]
		read := logger2.inMemEntries[idx]
		if wrote.Term != read.Term || wrote.Index != read.Index || string(wrote.Command) != string(read.Command) {
			t.Fatalf("expected entries to be equal")
		}
	}
}
