package storage

import (
	"errors"

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
