package raft

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	// Log types
	USER_LOG              = 1
	RAFT_LOG              = 2
	HEARTBEAT             = 4
	CLUSTER_CHANGE_ADD    = 8
	CLUSTER_CHANGE_REMOVE = 16
)

const RAFT_COMMAND_DELIMITER = "|"

var (
	InvalidIndexError    = errors.New("invalid index")
	FileError            = errors.New("file error")
	InvalidSnapShotError = errors.New("invalid snapshot")
)

const (
	logFileName          = "raft.log"
	configFileName       = "logger.conf"
	logEntryDataSplitter = ","
	logEntryFormat       = "%d,%d,%d,%s"
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

func formatRaftLogCommand(commands ...string) string {
	return strings.Join(commands, RAFT_COMMAND_DELIMITER)
}

func StringToLogEntry(s string) (LogEntry, error) {
	var term, index, logType uint64
	var command string
	var err error
	components := strings.Split(s, logEntryDataSplitter)
	if len(components) != 4 {
		return LogEntry{}, FileError
	}
	if term, err = strconv.ParseUint(components[0], 10, 64); err != nil {
		return LogEntry{}, FileError
	}
	if index, err = strconv.ParseUint(components[1], 10, 64); err != nil {
		return LogEntry{}, FileError
	}
	if logType, err = strconv.ParseUint(components[2], 10, 64); err != nil {
		return LogEntry{}, FileError
	}
	command = components[3]
	return LogEntry{Term: term, Index: index, Type: uint32(logType), Command: []byte(command)}, nil
}

type Logger interface {
	// Truncate the log from the beginning to the given index (inclusive)
	TruncateTo(index uint64) error
	// Remove all logs from the beginning to the given index (inclusive)
	Cut(index uint64) error
	// Returns true if the log is empty
	Empty() bool
	// Get the term of the last log entry
	LastLogTerm() uint64
	// Get the index of the last log entry
	LastLogIndex() uint64
	// Get the log entry at the given index
	Get(index uint64) (LogEntry, error)
	// Get all log entries with index in the range [start, end)
	GetRange(start uint64, end uint64) ([]LogEntry, error)
	// Append log entries to the log
	Append(entries []LogEntry) error
}

type LoggerConfig struct {
	LastSnapshotName string
	SnapshotsInfo    map[string]SnapshotInfo
}

func (c *LoggerConfig) Serialize() ([]byte, error) {
	return json.Marshal(c)
}

type LoggerImplem struct {
	inMemEntries     []LogEntry
	inMemSize        uint64
	logFile          *os.File
	logSeperatorChar byte

	StateMachine
	confDir string
	config  LoggerConfig
}

func (l *LoggerImplem) logError(msg string) {
	log.Println("Logger error: ", msg)
}

func NewLoggerImplem(sm StateMachine, confDir string, seperatorChar byte) *LoggerImplem {
	implem := LoggerImplem{
		inMemEntries:     make([]LogEntry, 0),
		inMemSize:        0,
		logSeperatorChar: '\n',
		StateMachine:     sm,
		confDir:          confDir,
		config:           LoggerConfig{SnapshotsInfo: make(map[string]SnapshotInfo)},
	}
	implem.initialize()
	return &implem
}

func (l *LoggerImplem) writeOutLogs(logs []LogEntry) error {
	wroutContent := make([]byte, 0, 1024)
	for _, entry := range logs {
		line := fmt.Sprintf("%s%c", entry.String(), l.logSeperatorChar)
		wroutContent = append(wroutContent, line...)
	}
	newLogFile, err := os.Create(l.logFile.Name() + ".tmp")
	if err != nil {
		return FileError
	}
	defer func() {
		newLogFile.Close()
		os.Remove(newLogFile.Name())
	}()
	_, err = newLogFile.Write(wroutContent)
	if err != nil {
		return FileError
	}
	if os.Truncate(l.logFile.Name(), 0) != nil {
		return FileError
	}
	l.logFile.Seek(0, 0)
	newLogFile.Seek(0, 0)
	totalWrt := 0
	for {
		wrt, err := io.Copy(l.logFile, newLogFile)
		totalWrt += int(wrt)
		if err == io.EOF || wrt == 0 {
			break
		}
		if err != nil {
			l.logError("Error copying log file: " + err.Error())
			return FileError
		}
	}
	fmt.Println("new length: ", len(l.inMemEntries), "totalWrt: ", totalWrt)
	return nil
}

func (l *LoggerImplem) TruncateTo(index uint64) error {
	startIndex := l.inMemEntries[0].Index
	offset := index - startIndex
	if index < startIndex || offset+1 > uint64(len(l.inMemEntries)) {
		return InvalidIndexError
	}
	l.inMemEntries = l.inMemEntries[:index-startIndex+1]
	return l.writeOutLogs(l.inMemEntries)
}

func (l *LoggerImplem) Cut(index uint64) error {
	startIndex := l.inMemEntries[0].Index
	offset := index - startIndex
	if index < startIndex || offset+1 > uint64(len(l.inMemEntries)) {
		return InvalidIndexError
	}
	l.inMemEntries = l.inMemEntries[index-startIndex+1:]
	return l.writeOutLogs(l.inMemEntries)
}

func (l *LoggerImplem) LastLogTerm() uint64 {
	return l.inMemEntries[len(l.inMemEntries)-1].Term
}

func (l *LoggerImplem) LastLogIndex() uint64 {
	return l.inMemEntries[len(l.inMemEntries)-1].Index
}

func (l *LoggerImplem) Empty() bool {
	return len(l.inMemEntries) == 0
}

func (l *LoggerImplem) Get(index uint64) (LogEntry, error) {
	startIndex := l.inMemEntries[0].Index
	offset := index - startIndex
	if index < startIndex || offset+1 > uint64(len(l.inMemEntries)) {
		return LogEntry{}, InvalidIndexError
	}
	return l.inMemEntries[offset], nil
}

func (l *LoggerImplem) GetRange(start uint64, end uint64) ([]LogEntry, error) {
	startOffset := start - l.inMemEntries[0].Index
	endOffset := end - l.inMemEntries[0].Index
	logsArrayLen := uint64(len(l.inMemEntries))
	if start < 0 || startOffset+1 > logsArrayLen {
		return nil, InvalidIndexError
	}
	if end < 0 || endOffset+1 > logsArrayLen {
		return nil, InvalidIndexError
	}
	return l.inMemEntries[startOffset : endOffset+1], nil
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
	// Seek to the end of the file
	l.logFile.Seek(0, 2)
	_, err := l.logFile.Write(content)
	if err != nil {
		log.Println("Error writing to log file: ", err)
		return FileError
	}
	return nil
}



func (l *LoggerImplem) createConfDir() error {
	err := os.MkdirAll(l.confDir, 0777)
	if err != nil {
		l.logError("Error creating conf dir: " + err.Error() + "confdir: " + l.confDir)
		return err
	}
	return nil
}

func (l *LoggerImplem) checkLogFile() (bool, error) {
	_, err := os.Stat(l.confDir + "/" + logFileName)
	if err != nil {
		_, err := os.Create(l.confDir + "/" + logFileName)
		if err != nil {
			l.logError("Error creating log file: " + err.Error())
			return false, err
		}
	}
	return true, nil
}

func (l *LoggerImplem) initialize() error {
	log.Println("Initializing logger", l.confDir)
	if l.createConfDir() != nil {
		return FileError
	}
	_, err := l.checkLogFile()
	if err != nil {
		l.logError("Error checking log file: " + err.Error())
		return err
	}
	file, err := os.OpenFile(l.confDir+"/"+logFileName, os.O_RDWR, 0777)
	if err != nil {
		l.logError("Error opening log file: " + err.Error())
		return err
	}
	l.logFile = file
	err = l.readLogFile()
	if err != nil {
		return err
	}

	// At this point:
	// - logFile is open and ready for writing
	// - inMemEntries contains all log entries
	return nil
}

func (l *LoggerImplem) readLogFile() error {
	scanner := bufio.NewScanner(l.logFile)
	scanner.Split(bufio.ScanBytes)
	var line []byte = make([]byte, 0, 1024)
	for scanner.Scan() {
		b := scanner.Bytes()
		if b[0] != l.logSeperatorChar {
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
	_, err := l.logFile.Seek(0, 0)
	if err != nil {
		return FileError
	}
	return nil
}


func (l *LoggerImplem) saveConfig() error {
	out, err := l.config.Serialize()
	if err != nil {
		return err
	}
	configFile, err := os.Create(l.confDir + "/" + configFileName)
	if err != nil {
		return err
	}
	defer configFile.Close()
	_, err = configFile.Write(out)
	return err
}

func (l *LoggerImplem) loadConfig() error {
	configFileName := l.confDir + "/" + configFileName
	configFile, err := os.Open(configFileName)
	if err != nil {
		return err
	}
	defer configFile.Close()
	info, _ := configFile.Stat()
	bytes := make([]byte, info.Size())
	_, err = configFile.Read(bytes)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bytes, &l.config)
	return err
}


