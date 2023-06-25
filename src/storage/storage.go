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

var ErrIndexOutOfBounds = errors.New("index out of bounds")

type State struct {
	CurrentTerm uint64
	VotedFor    types.ReplicaID
}

type Storage interface {
	Directory() string

	GetState() (*State, error)

	Persist(state State) error

	AppendEntries(entries []types.Entry) error

	TruncateLogStartingFrom(index uint64) error

	// 1-based indexing. First entry starts at 1.
	GetEntryAtIndex(index uint64) (*types.Entry, error)

	// 1-based indexing.
	GetBatch(startingIndex uint64, batchSized uint64) ([]types.Entry, error)

	// 1-based indexing. First entry starts at 1. 0 means empty.
	LastLogIndex() uint64

	// 1-based indexing. First entry starts at 1. 0 means empty.
	LastLogTerm() uint64

	Debug() []types.Entry
}

type FileStorage struct {
	stateFile *os.File
	state     *State
	entries   []types.Entry
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

	state, err := getState(stateFile)
	if err != nil {
		return nil, fmt.Errorf("getting state from disk: %w", err)
	}

	return &FileStorage{
		stateFile: stateFile,
		state:     state,
		entries:   make([]types.Entry, 0),
		directory: directory,
	}, nil
}

func getState(file *os.File) (*State, error) {
	// 8 bytes for the current term
	// 2 bytes for the current vote
	var buffer [10]byte
	n, err := file.ReadAt(buffer[:], 0)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading state file: %w", err)
	}
	if n != len(buffer) {
		return nil, fmt.Errorf("read unexpected number of bytes from state file: bytesRead=%d", n)
	}

	currentTerm := binary.LittleEndian.Uint64(buffer[0:8])
	votedFor := binary.LittleEndian.Uint16(buffer[8:10])

	return &State{CurrentTerm: currentTerm, VotedFor: uint16(votedFor)}, nil
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
	storage.entries = append(storage.entries, entries...)

	return nil
}

func (storage *FileStorage) TruncateLogStartingFrom(index uint64) error {
	assert.True(index > 0, "storage is 1-indexed")

	if index == 1 {
		storage.entries = make([]types.Entry, 0)
	} else {
		storage.entries = storage.entries[:index-1]
	}

	return nil
}

func (storage *FileStorage) GetEntryAtIndex(index uint64) (*types.Entry, error) {
	assert.True(index > 0, "storage is 1-indexed")

	if index > uint64(len(storage.entries)) {
		return nil, fmt.Errorf("index out of bounds: lastEntryIndex=%d index=%d %w", len(storage.entries), index, ErrIndexOutOfBounds)
	}

	// Index is 1-based.
	return &storage.entries[index-1], nil
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
	return uint64(len(storage.entries))
}

func (storage *FileStorage) LastLogTerm() uint64 {
	if storage.isLogEmpty() {
		return 0
	}

	entry, err := storage.GetEntryAtIndex(uint64(len(storage.entries)))
	if err != nil {
		panic(err)
	}

	return entry.Term
}

func (storage *FileStorage) isLogEmpty() bool {
	return len(storage.entries) == 0
}

func (storage *FileStorage) Debug() []types.Entry {
	return storage.entries
}
