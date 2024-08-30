package raft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

const (
	// Log types
	USER_LOG              = 1
	RAFT_LOG              = 2
	HEARTBEAT             = 4
	CLUSTER_CHANGE_ADD    = 8
	CLUSTER_CHANGE_REMOVE = 16
)

var (
	InvalidIndexError = errors.New("invalid index")
	FileError         = errors.New("file error")
)

const (
	logEntryFormat = "%d,%d,%d,%s"
)

type LogEntry struct {
	Term    uint64
	Index   uint64
	Type    uint32
	Command []byte
}

func (l LogEntry) String() string {
	return fmt.Sprintf(logEntryFormat, l.Term, l.Index, l.Type, string(l.Command))
}

func StringToLogEntry(s string) (LogEntry, error) {
	var term, index, logType uint64
	var command string
	n, err := fmt.Sscanf(s, logEntryFormat, &term, &index, &logType, &command)
	if n != 4 || err != nil {
		return LogEntry{}, FileError
	}
	return LogEntry{Term: term, Index: index, Type: uint32(logType), Command: []byte(command)}, nil
}

type Logger interface {
	// Truncate the log from the beginning to the given index (exclusive)
	TruncateTo(index uint64) error
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
	// Initialize the logger from an existing log file
	// used on node start up / recovery
	Initialize(logFileName string) error
}

type LoggerImplem struct {
	inMemEntries     []LogEntry
	inMemSize        uint64
	logFile          *os.File
	logSeperatorChar byte
}

func NewLoggerImplem() *LoggerImplem {
	return &LoggerImplem{
		inMemEntries:     make([]LogEntry, 0),
		inMemSize:        0,
		logFile:          nil,
		logSeperatorChar: '\n',
	}
}

func (l *LoggerImplem) TruncateTo(index uint64) error {
	startIndex := l.inMemEntries[0].Index
	endIndex := l.inMemEntries[len(l.inMemEntries)-1].Index
	if index < startIndex || index > endIndex {
		return InvalidIndexError
	}
	l.inMemEntries = l.inMemEntries[:index-startIndex]
	wroutContent := make([]byte, 0, 1024)
	for _, entry := range l.inMemEntries {
		line := fmt.Sprintf("%d%d%d%s%c\n", entry.Term, entry.Index, entry.Type, string(entry.Command), l.logSeperatorChar)
		wroutContent = append(wroutContent, line...)
	}
	newLogFile, err := os.Create(l.logFile.Name() + ".tmp")
	if err != nil {
		return FileError
	}
	defer newLogFile.Close()
	_, err = newLogFile.Write(wroutContent)
	if err != nil {
		return FileError
	}
	_, err = io.Copy(newLogFile, l.logFile)
	if err != nil {
		return FileError
	}
	return nil
}

func (l *LoggerImplem) LastLogTerm() uint64 {
	return l.inMemEntries[len(l.inMemEntries)-1].Term
}

func (l *LoggerImplem) LastLogIndex() uint64 {
	return l.inMemEntries[len(l.inMemEntries)-1].Index
}

func (l *LoggerImplem) Get(index uint64) (LogEntry, error) {
	startIndex := l.inMemEntries[0].Index
	endIndex := l.inMemEntries[len(l.inMemEntries)-1].Index
	if index < startIndex || index > endIndex {
		return LogEntry{}, InvalidIndexError
	}
	return l.inMemEntries[index-startIndex], nil
}

func (l *LoggerImplem) GetFrom(start uint64) ([]LogEntry, error) {
	startIndex := l.inMemEntries[0].Index
	endIndex := l.inMemEntries[len(l.inMemEntries)-1].Index
	if start < startIndex || start > endIndex {
		return nil, InvalidIndexError
	}
	ret := make([]LogEntry, startIndex-endIndex)
	copy(ret, l.inMemEntries)
	return ret, nil
}

func (l *LoggerImplem) GetRange(start uint64, end uint64) ([]LogEntry, error) {
	startIndex := l.inMemEntries[0].Index
	endIndex := l.inMemEntries[len(l.inMemEntries)-1].Index
	if start < startIndex || start > endIndex || end < start || end > endIndex {
		return nil, InvalidIndexError
	}
	return l.inMemEntries[start-startIndex : end-startIndex], nil
}

func (l *LoggerImplem) Append(entries []LogEntry) error {
	l.inMemEntries = append(l.inMemEntries, entries...)
	wroutContent := make([]byte, 0, 1024)
	for _, entry := range entries {
		line := fmt.Sprintf("%s%c", entry.String(), l.logSeperatorChar)
		wroutContent = append(wroutContent, line...)
	}
	err := l.appendToLogFile(wroutContent)
	if err != nil {
		return err
	}
	return nil
}

func (l *LoggerImplem) appendToLogFile(content []byte) error {
	_, err := l.logFile.Write(content)
	if err != nil {
		log.Println("Error writing to log file: ", err)
		return FileError
	}
	return nil
}

func (l *LoggerImplem) Initialize(logFileName string, seperatorChar byte) error {
	l.inMemEntries = make([]LogEntry, 0, 1024)
	l.inMemSize = 0
	l.logSeperatorChar = seperatorChar
	logFile, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Println("Error opening log file: ", err)
		return FileError
	}
	l.logFile = logFile

	scanner := bufio.NewScanner(logFile)
	scanner.Split(bufio.ScanBytes)

	var line []byte = make([]byte, 0, 1024)
	for scanner.Scan() {
		b := scanner.Bytes()
		if b[0] != seperatorChar {
			line = append(line, b[0])
			continue
		}

		// Now we have a full line, process it
		entry, err := StringToLogEntry(string(line))
		if err != nil {
			log.Println("Error parsing log file: ", err, string(line))
			return FileError
		}
		l.inMemEntries = append(l.inMemEntries, entry)

		// Reset line for the next one
		line = line[:0]
	}
	if scanner.Err() != nil {
		return FileError
	}

	l.inMemSize = uint64(len(l.inMemEntries))
	_, err = l.logFile.Seek(0, 0)
	if err != nil {
		return FileError
	}
	return nil
}
