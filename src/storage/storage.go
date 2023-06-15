package storage

import (
	"errors"
	"fmt"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

var ErrIndexOutOfBounds = errors.New("index out of bounds")

type State struct {
	CurrentTerm uint64
	VotedFor    types.ReplicaID
}

type Storage interface {
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
}

type FileStorage struct {
	state   *State
	entries []types.Entry
}

func NewFileStorage() *FileStorage {
	return &FileStorage{
		entries: make([]types.Entry, 0),
	}
}

func (storage *FileStorage) GetState() (*State, error) {
	return storage.state, nil
}

func (storage *FileStorage) Persist(state State) error {
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
