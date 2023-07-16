package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

const entrySizeBytes = 8 + // the term
	8 + // the index
	2 + // the type
	2 + // the value length
	256 // the value

type FileStorage struct {
	stateFile      *os.File
	logFile        *os.File
	state          *State
	nextEntryIndex uint64

	// The directory where raft files are going to be stored.
	directory string
}

func NewFileStorage(directory string) (*FileStorage, error) {
	if err := os.MkdirAll(directory, 0750); err != nil {
		return nil, fmt.Errorf("creating storage directory: directory=%s %w", directory, err)
	}

	stateFilePath := path.Join(directory, "raft.state")
	stateFile, err := os.OpenFile(stateFilePath, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return nil, fmt.Errorf("opening state file: path=%s %w", stateFilePath, err)
	}

	logFilePath := path.Join(directory, "raft.log")
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return nil, fmt.Errorf("opening log file: path=%s %w", stateFilePath, err)
	}

	state, err := getState(stateFile)
	if err != nil {
		return nil, fmt.Errorf("getting state from disk: %w", err)
	}

	lastEntyIndex, err := lastEntryIndex(logFile)
	if err != nil {
		return nil, fmt.Errorf("getting last entry index: %w", err)
	}

	return &FileStorage{
		stateFile:      stateFile,
		logFile:        logFile,
		state:          state,
		nextEntryIndex: lastEntyIndex + 1,
		directory:      directory,
	}, nil
}

func lastEntryIndex(file *os.File) (uint64, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("executing file stat: %w", err)
	}

	fileSize := fileInfo.Size()

	// If there is 0 or 1 entry.
	if fileSize <= entrySizeBytes {
		// Entry starts at the first byte.
		return 0, nil
	}

	return uint64(fileSize) - entrySizeBytes, nil
}

func (storage *FileStorage) Directory() string {
	return storage.directory
}

func (storage *FileStorage) GetState() (*State, error) {
	return storage.state, nil
}

func (storage *FileStorage) Persist(state State) error {
	var buffer [10]byte

	binary.LittleEndian.PutUint64(buffer[0:8], state.CurrentTerm)
	binary.LittleEndian.PutUint16(buffer[8:10], state.VotedFor)

	_, err := storage.stateFile.WriteAt(buffer[:], 0)
	if err != nil {
		return fmt.Errorf("writing state to file: %w", err)
	}

	if err := storage.stateFile.Sync(); err != nil {
		return fmt.Errorf("syncing data to disk: %w", err)
	}

	storage.state = &state

	return nil
}

func (storage *FileStorage) AppendEntries(entries []types.Entry) error {
	buffer := make([]byte, 0, entrySizeBytes*len(entries))

	nextEntryIndex := storage.nextEntryIndex

	for i, entry := range entries {
		entry.Index = nextEntryIndex
		nextEntryIndex++

		buffer = binary.LittleEndian.AppendUint64(buffer, entry.Term)

		buffer = binary.LittleEndian.AppendUint64(buffer, entry.Index)

		buffer = binary.LittleEndian.AppendUint16(buffer, uint16(entry.Type))

		assert.True(len(entry.Value) <= entrySizeBytes, "max entry size reached")

		buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(entry.Value)))

		buffer = append(buffer, entry.Value...)

		entryStartsAt := i * entrySizeBytes

		entryLenInBuffer := len(buffer[entryStartsAt:])

		// Add padding.
		if entryLenInBuffer != entrySizeBytes {
			buffer = append(buffer, make([]byte, entrySizeBytes-entryLenInBuffer)...)
		}

		assert.True(len(buffer[entryStartsAt:]) == entrySizeBytes, "entry with wrong size in buffer")
	}

	assert.True(len(buffer) == entrySizeBytes*len(entries), "unexpected buffer size after adding all entries")

	_, err := storage.logFile.Write(buffer)
	if err != nil {
		return fmt.Errorf("writing entries to log file: %w", err)
	}

	if err := storage.logFile.Sync(); err != nil {
		return fmt.Errorf("syncing log file: %w", err)
	}

	storage.nextEntryIndex = nextEntryIndex

	return nil
}

func (storage *FileStorage) TruncateLogStartingFrom(index uint64) error {
	assert.True(index > 0, "storage is 1-indexed")

	if index >= storage.nextEntryIndex {
		return fmt.Errorf("index out of bounds: nextEntryIndex=%d index=%d %w", storage.nextEntryIndex, index, ErrIndexOutOfBounds)
	}

	// index-1 to make sure we start at
	entryStartsAtIndex := (index - 1) * entrySizeBytes

	if err := storage.logFile.Truncate(int64(entryStartsAtIndex)); err != nil {
		return fmt.Errorf("truncating log file: offset=%d %w", entryStartsAtIndex, err)
	}

	if _, err := storage.logFile.Seek(int64(entryStartsAtIndex), io.SeekStart); err != nil {
		return fmt.Errorf("seeking after truncating file: %w", err)
	}

	if err := storage.logFile.Sync(); err != nil {
		return fmt.Errorf("syncing log file: %w", err)
	}

	storage.nextEntryIndex = index

	return nil
}

func (storage *FileStorage) GetEntryAtIndex(index uint64) (*types.Entry, error) {
	assert.True(index > 0, "storage is 1-indexed")

	if index >= storage.nextEntryIndex {
		return nil, fmt.Errorf("index out of bounds: nextEntryIndex=%d index=%d %w", storage.nextEntryIndex, index, ErrIndexOutOfBounds)
	}

	// index starts at 1. subtract 1 to ensure we start reading from byte 0.
	entryStartsAtIndex := int64((index - 1) * entrySizeBytes)

	buffer := make([]byte, entrySizeBytes)

	if _, err := storage.logFile.ReadAt(buffer, entryStartsAtIndex); err != nil {
		return nil, fmt.Errorf("reading log entry to buffer: %w", err)
	}

	term := binary.LittleEndian.Uint64(buffer)

	entryIndex := binary.LittleEndian.Uint64(buffer[8:])

	typ := binary.LittleEndian.Uint16(buffer[16:])

	valueLength := binary.LittleEndian.Uint16(buffer[18:])

	value := buffer[20 : 20+valueLength]

	return &types.Entry{Term: term, Index: entryIndex, Type: uint8(typ), Value: value}, nil
}

func (storage *FileStorage) GetBatch(startingIndex uint64, batchSize uint64) ([]types.Entry, error) {
	assert.True(startingIndex > 0, "storage is 1-indexed")

	out := make([]types.Entry, 0, batchSize)

	for i := 0; i < int(batchSize); i++ {
		index := startingIndex + uint64(i)

		entry, err := storage.GetEntryAtIndex(index)
		if errors.Is(err, ErrIndexOutOfBounds) {
			return out, nil
		}
		if err != nil {
			return out, fmt.Errorf("fetching entry at index: index=%d %w", index, err)
		}

		out = append(out, *entry)
	}

	return out, nil
}

func (storage *FileStorage) LastLogIndex() uint64 {
	return storage.nextEntryIndex - 1
}

// func (storage *FileStorage) LastLogTerm() uint64 {
// 	if storage.isLogEmpty() {
// 		return 0
// 	}

// 	entry, err := storage.GetEntryAtIndex(uint64(len(storage.entries)))
// 	if err != nil {
// 		panic(err)
// 	}

// 	return entry.Term
// }

// func (storage *FileStorage) Debug() []types.Entry {
// 	return storage.entries
// }
