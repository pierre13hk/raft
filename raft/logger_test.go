package raft

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

func TestTruncateEndDebug(t *testing.T) {
	logger := &InMemoryLogger{
		entries: []LogEntry{
			{Term: 1, Index: 0, Command: []byte("a")},
			{Term: 1, Index: 1, Command: []byte("b")},
			{Term: 1, Index: 2, Command: []byte("c")},
		},
	}

	logger.TruncateTo(1)

	if len(logger.entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(logger.entries))
	}

	if logger.entries[1].Index != 1 {
		t.Fatalf("expected second entry to be index 1, got %d", logger.entries[0].Index)
	}

	logger.TruncateTo(0)

	if len(logger.entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(logger.entries))
	}

	if logger.entries[0].Index != 0 {
		t.Fatalf("expected first entry to be index 1, got %d", logger.entries[0].Index)
	}
}

var tmpLogDir = "./tmp/"

func newLogger(t *testing.T) *LoggerImplem {
	randomInt := rand.Intn(100)
	tmpDir := t.TempDir()
	tmpLogDir = tmpDir + "/"
	logger := NewLoggerImplem(
		&DebugStateMachine{},
		tmpLogDir+strconv.Itoa(randomInt),
		'\n',
	)
	return logger
}

func TestLoggerAppend(t *testing.T) {
	logger := newLogger(t)

	err := logger.Append([]LogEntry{
		{Term: 1, Index: 0, Command: []byte("a")},
		{Term: 1, Index: 1, Command: []byte("b")},
		{Term: 1, Index: 2, Command: []byte("c")},
	})
	if err != nil {
		fmt.Println(err)
		t.Fatalf("expected entries to be appended")
	}
	if logger.LastLogIndex() != 2 {
		t.Fatalf("expected last log index to be 2, got %d", logger.LastLogIndex())
	}
	if logger.LastLogTerm() != 1 {
		t.Fatalf("expected last log term to be 1, got %d", logger.LastLogTerm())
	}

	logger2 := NewLoggerImplem(
		&DebugStateMachine{},
		logger.confDir,
		'\n',
	)
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
			t.Fatalf("expected entries to be equal wrote: %v read: %v", wrote, read)
		}
	}
}

func TestLoggerTruncateSimple(t *testing.T) {
	logger := newLogger(t)

	err := logger.Append([]LogEntry{
		{Term: 1, Index: 0, Command: []byte("a")},
		{Term: 1, Index: 1, Command: []byte("b")},
		{Term: 1, Index: 2, Command: []byte("c")},
	})
	if err != nil {
		t.Fatalf("expected entries to be appended")
	}

	err = logger.TruncateTo(1)
	if err != nil {
		t.Fatalf("expected entries to be truncated")
	}

	if len(logger.inMemEntries) != 2 {
		t.Fatalf("expected 1 entries, got %d %v", len(logger.inMemEntries), logger.inMemEntries)
	}

	if logger.inMemEntries[0].Index != 0 {
		t.Fatalf("expected first entry to be index 0, got %d", logger.inMemEntries[0].Index)
	}
	logger2 := NewLoggerImplem(
		&DebugStateMachine{},
		logger.confDir,
		'\n',
	)
	if len(logger2.inMemEntries) != 2 {
		t.Fatalf("expected 1 entries, got %d", len(logger2.inMemEntries))
	}
}

func TestLoggerTruncateOffset(t *testing.T) {
	logger := newLogger(t)

	err := logger.Append([]LogEntry{
		{Term: 1, Index: 3, Command: []byte("a")},
		{Term: 1, Index: 4, Command: []byte("b")},
		{Term: 1, Index: 5, Command: []byte("c")},
	})
	if err != nil {
		t.Fatalf("expected entries to be appended")
	}

	err = logger.TruncateTo(4)
	if err != nil {
		t.Fatalf("expected entries to be truncated")
	}

	if len(logger.inMemEntries) != 2 {
		t.Fatalf("expected 1 entries, got %d %v", len(logger.inMemEntries), logger.inMemEntries)
	}

	if logger.inMemEntries[0].Index != 3 {
		t.Fatalf("expected first entry to be index 0, got %d", logger.inMemEntries[0].Index)
	}
	logger2 := NewLoggerImplem(
		&DebugStateMachine{},
		logger.confDir,
		'\n',
	)
	if len(logger2.inMemEntries) != 2 {
		t.Fatalf("expected 1 entries, got %d", len(logger2.inMemEntries))
	}
}

func TestLoggerCutSimple(t *testing.T) {
	logger := newLogger(t)

	err := logger.Append([]LogEntry{
		{Term: 1, Index: 0, Command: []byte("a")},
		{Term: 1, Index: 1, Command: []byte("b")},
		{Term: 1, Index: 2, Command: []byte("c")},
	})
	if err != nil {
		fmt.Println(err)
		t.Fatalf("expected entries to be appended")
	}

	err = logger.Cut(0)
	if err != nil {
		t.Fatalf("expected entries to be cut")
	}
	if logger.inMemEntries[0].Index != 1 {
		t.Fatalf("expected first entry to be index 1, got %d", logger.inMemEntries[0].Index)
	}
	if len(logger.inMemEntries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(logger.inMemEntries))
	}

	logger2 := NewLoggerImplem(
		&DebugStateMachine{},
		logger.confDir,
		'\n',
	)
	if len(logger2.inMemEntries) != 2 {
		t.Fatalf("logger 2expected 2 entries, got %d, not writing to disk", len(logger2.inMemEntries))
	}
}

func TestLoggerCutOffset(t *testing.T) {
	logger := newLogger(t)

	err := logger.Append([]LogEntry{
		{Term: 1, Index: 3, Command: []byte("a")},
		{Term: 1, Index: 4, Command: []byte("b")},
		{Term: 1, Index: 5, Command: []byte("c")},
	})
	if err != nil {
		t.Fatalf("expected entries to be appended")
	}

	err = logger.Cut(4)
	if err != nil {
		t.Fatalf("expected entries to be cut")
	}
	if logger.inMemEntries[0].Index != 5 {
		t.Fatalf("expected first entry to be index 5, got %d", logger.inMemEntries[0].Index)
	}
	if len(logger.inMemEntries) != 1 {
		t.Fatalf("expected 1 entries, got %d", len(logger.inMemEntries))
	}

	logger2 := NewLoggerImplem(
		&DebugStateMachine{},
		logger.confDir,
		'\n',
	)
	if len(logger2.inMemEntries) != 1 {
		t.Fatalf("expected 1 entries, got %d", len(logger2.inMemEntries))
	}

	err = logger.Cut(2)
	if err == nil {
		t.Fatalf("expected error when cutting at index 2")
	}

	err = logger.Cut(6)
	if err == nil {
		t.Fatalf("expected error when cutting at index 6")
	}

	err = logger.Cut(5)
	if err != nil {
		t.Fatalf("expected entries to be cut")
	}
	if len(logger.inMemEntries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(logger.inMemEntries))
	}

}

func TestGetInitial(t *testing.T) {
	logger := newLogger(t)

	err := logger.Append([]LogEntry{
		{Term: 0, Index: 0, Command: []byte("a")},
	})
	if err != nil {
		t.Fatalf("expected entries to be appended")
	}

	entry, err := logger.Get(0)
	if err != nil {
		t.Fatalf("shouldn have gotten entry :%v", entry)
	}
}

func TestGet(t *testing.T) {
	logger := newLogger(t)

	err := logger.Append([]LogEntry{
		{Term: 1, Index: 3, Command: []byte("a")},
		{Term: 1, Index: 4, Command: []byte("b")},
		{Term: 1, Index: 5, Command: []byte("c")},
	})
	if err != nil {
		t.Fatalf("expected entries to be appended")
	}

	entry, err := logger.Get(1)
	if err == nil {
		t.Fatalf("shouldn't have gotten entry :%v", entry)
	}

	entry, err = logger.Get(3)
	if err != nil {
		t.Fatalf("expected entry to be retrieved")
	}
	if entry.Index != 3 {
		t.Fatalf("expected entry index to be 3, got %d", entry.Index)
	}

	entry, err = logger.Get(5)
	if err != nil {
		t.Fatalf("expected entry to be retrieved")
	}
	if entry.Index != 5 {
		t.Fatalf("expected entry index to be 5, got %d", entry.Index)
	}

	_, err = logger.Get(6)
	if err == nil {
		t.Fatalf("shouldn't have gotten entry")
	}
}

func TestCreateSnapshot(t *testing.T) {
	logger := newLogger(t)

	err := logger.Append([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
	})
	if err != nil {
		t.Fatalf("expected entries to be appended")
	}
	err = logger.CreateSnapshot(3)
	if err != nil {
		t.Fatalf("thought snapshot creation would work")
	}

	logger2 := NewLoggerImplem(
		&DebugStateMachine{},
		logger.confDir,
		'\n',
	)
	if len(logger2.inMemEntries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(logger2.inMemEntries))
	}
	for idx, _ := range logger2.inMemEntries {
		wrote := logger.inMemEntries[idx]
		read := logger2.inMemEntries[idx]
		if wrote.Term != read.Term || wrote.Index != read.Index || string(wrote.Command) != string(read.Command) {
			t.Fatalf("expected entries to be equal")
		}
	}
}

func TestLogFileCursor(t *testing.T) {
	// Test that the cursor for reading and
	// writing to the log file works as expected
	logger := newLogger(t)
	err := logger.Append([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
	})
	if err != nil {
		t.Fatalf("expected entries to be appended")
	}
	err = logger.Cut(2)
	if err != nil {
		t.Fatalf("expected entries to be cut")
	}
	err = logger.Append([]LogEntry{
		{Term: 1, Index: 4, Command: []byte("d")},
		{Term: 1, Index: 5, Command: []byte("e")},
	})
	if err != nil {
		t.Fatalf("expected entries to be appended")
	}
	err = logger.TruncateTo(4)
	if err != nil {
		t.Fatalf("expected entries to be truncated")
	}
	// The log should only have the log with indexes 4 and 5
	logger2 := NewLoggerImplem(
		&DebugStateMachine{},
		logger.confDir,
		'\n',
	)
	fmt.Println(logger2.inMemEntries)
	if len(logger2.inMemEntries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(logger2.inMemEntries))
	}
	if logger2.inMemEntries[0].Index != 3 {
		t.Fatalf("expected first entry to be index 5, got %d", logger2.inMemEntries[0].Index)
	}
	if logger2.inMemEntries[1].Index != 4 {
		t.Fatalf("expected second entry to be index 4, got %d", logger2.inMemEntries[1].Index)
	}
}

func TestReadWrite(t *testing.T) {
	logger := newLogger(t)
	err := logger.Append([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("a 1 * 3")},
		{Term: 1, Index: 2, Command: []byte("b 2 * 3")},
		{Term: 1, Index: 3, Command: []byte("c 3 * 3")},
	})
	if err != nil {
		t.Fatalf("expected entries to be appended")
	}

	logger2 := NewLoggerImplem(
		&DebugStateMachine{},
		logger.confDir,
		'\n',
	)
	if len(logger2.inMemEntries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(logger2.inMemEntries))
	}
	for idx, _ := range logger2.inMemEntries {
		wrote := logger.inMemEntries[idx]
		read := logger2.inMemEntries[idx]
		if wrote.Term != read.Term || wrote.Index != read.Index {
			t.Fatalf("expected entries log info to be equal")
		}
		if string(wrote.Command) != string(read.Command) {
			t.Fatalf("expected entry commands to be equal, want:[%s] have:[%s]", string(wrote.Command), string(read.Command))
		}
	}
}

func TestFormatRaftLogCommand(t *testing.T) {
	// Test that the formatRaftLogCommand function
	// formats the command as expected
	expected := "a|1|*|3"
	formatted := formatRaftLogCommand("a", "1", "*", "3")
	if formatted != expected {
		t.Fatalf("expected formatted command to be 'a 1 * 3', got %s", formatted)
	}
}
