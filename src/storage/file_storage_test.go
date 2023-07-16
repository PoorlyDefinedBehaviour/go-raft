package storage

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

type Model struct {
	entries []types.Entry
}

func newModel() *Model {
	return &Model{}
}

func (model *Model) AppendEntries(entries []types.Entry) error {
	for _, entry := range entries {
		entry.Index = uint64(len(model.entries) + 1)
		model.entries = append(model.entries, entry)
	}
	return nil
}

func (model *Model) GetEntryAtIndex(index uint64) (*types.Entry, error) {
	// The storage is 1-based. Subtract 1 to map to the slice we have.
	realIndex := index - 1

	if realIndex >= uint64(len(model.entries)) {
		return nil, ErrIndexOutOfBounds
	}

	return &model.entries[realIndex], nil
}

func (model *Model) TruncateLogStartingFrom(index uint64) error {
	// The storage is 1-based. Subtract 1 to map to the slice we have.
	realIndex := index - 1

	if realIndex >= uint64(len(model.entries)) {
		return ErrIndexOutOfBounds
	}

	if realIndex == 0 {
		model.entries = make([]types.Entry, 0)
	} else {
		model.entries = model.entries[:realIndex]
	}

	return nil
}

func (model *Model) GetBatch(startingIndex uint64, batchSize uint64) ([]types.Entry, error) {
	batch := make([]types.Entry, 0, batchSize)

	for i := uint64(0); i < batchSize; i++ {
		entry, err := model.GetEntryAtIndex(startingIndex + i)
		if err != nil {
			if errors.Is(err, ErrIndexOutOfBounds) {
				return batch, nil
			}

			return batch, err
		}
		batch = append(batch, *entry)
	}

	return batch, nil
}

func (model *Model) LastLogIndex() uint64 {
	return uint64(len(model.entries))
}

func TestModel(t *testing.T) {
	t.Parallel()

	t.Run("get entry at index", rapid.MakeCheck(func(t *rapid.T) {
		model := newModel()

		entries := rapid.SliceOf(entryGenerator()).
			Filter(func(entries []types.Entry) bool {
				return len(entries) > 0
			}).
			Draw(t, "entries")

		assert.NoError(t, model.AppendEntries(entries))

		index := rapid.Uint64Range(1, uint64(len(entries))).Draw(t, "index")

		entry, err := model.GetEntryAtIndex(index)
		assert.NoError(t, err)

		expected := entries[index-1]
		expected.Index = index

		assert.Equal(t, expected, *entry)
	}))
}

func TestFileStorage(t *testing.T) {
	t.Parallel()

	const (
		OpAppendEntries           = "AppendEntries"
		OpTruncateLogStartingFrom = "TruncateLogStartingFrom"
		OpGetEntryAtIndex         = "GetEntryAtIndex"
		OpGetBatch                = "GetBatch"
		OpLastLogIndex            = "LastLogIndex"
	)

	rapid.Check(t, func(t *rapid.T) {
		storage, err := NewFileStorage(fmt.Sprintf("%s/raft-go/%s", os.TempDir(), uuid.New().String()))
		assert.NoError(t, err)

		model := newModel()

		ops := rapid.SliceOf(rapid.SampledFrom([]string{
			OpAppendEntries,
			OpTruncateLogStartingFrom,
			OpGetEntryAtIndex,
			OpGetBatch,
			OpLastLogIndex,
		})).
			Draw(t, "ops")

		for _, op := range ops {
			switch op {
			case OpAppendEntries:
				entries := rapid.SliceOf(entryGenerator()).Draw(t, "append: entries")
				assert.NoError(t, storage.AppendEntries(entries))
				assert.NoError(t, model.AppendEntries(entries))

				t.Logf("appended %d entries\n", len(entries))

			case OpTruncateLogStartingFrom:
				maybeExistingIndex := rapid.Uint64Range(1, uint64(len(model.entries)*2)+1).Draw(t, "truncate: maybeExistingIndex")

				storageErr := storage.TruncateLogStartingFrom(maybeExistingIndex)
				modelErr := model.TruncateLogStartingFrom(maybeExistingIndex)

				t.Logf("truncated from index %d len(model.entries)=%+v storageErr=%+v modelErr=%+v\n", maybeExistingIndex, len(model.entries), storageErr, modelErr)

				assert.ErrorIs(t, storageErr, modelErr)

			case OpGetEntryAtIndex:
				maybeExistingIndex := rapid.Uint64Range(1, uint64(len(model.entries)*2)+1).Draw(t, "get entry: maybeExistingIndex")

				storageEntry, storageErr := storage.GetEntryAtIndex(maybeExistingIndex)
				modelEntry, modelErr := model.GetEntryAtIndex(maybeExistingIndex)

				t.Logf("getting entry at index %d storageErr=%+v modelErr=%+v\n", maybeExistingIndex, storageErr, modelErr)

				assert.Equal(t, modelEntry, storageEntry)
				assert.ErrorIs(t, storageErr, modelErr)

			case OpGetBatch:
				maybeExistingIndex := rapid.Uint64Range(1, uint64(len(model.entries)*2)+1).Draw(t, "get batch: maybe existing index")
				batchSize := rapid.Uint64Range(1, uint64(len(model.entries)*2)+1).Draw(t, "get batch: batch size")

				storageBatch, storageErr := storage.GetBatch(maybeExistingIndex, batchSize)
				modelBatch, modelErr := model.GetBatch(maybeExistingIndex, batchSize)

				t.Logf("get batch index=%d batchSize=%d len(storageBatch)=%+v storageErr=%+v len(modelBatch)=%+v modelErr=%+v\n",
					maybeExistingIndex,
					batchSize,
					len(storageBatch),
					storageErr,
					len(modelBatch),
					modelErr,
				)

				assert.ErrorIs(t, storageErr, modelErr)
				assert.Equal(t, modelBatch, storageBatch)

			case OpLastLogIndex:
				storageLastLogIndex := storage.LastLogIndex()
				modelLastLogIndex := model.LastLogIndex()

				assert.Equal(t, modelLastLogIndex, storageLastLogIndex)

			default:
				panic(fmt.Sprintf("unexpected op: %s", op))
			}
		}
	})
}

func TestAppendEntries(t *testing.T) {
	t.Parallel()

	storage, err := NewFileStorage(fmt.Sprintf("%s/raft-go/%s", os.TempDir(), uuid.New().String()))
	assert.NoError(t, err)

	assert.NoError(t, storage.AppendEntries([]types.Entry{
		{
			Term:  1,
			Type:  1,
			Value: []byte("value 1"),
		},
		{
			Term:  2,
			Type:  2,
			Value: []byte("value 2"),
		},
	}))

	entry, err := storage.GetEntryAtIndex(1)
	assert.NoError(t, err)
	assert.Equal(t, types.Entry{Term: 1, Type: 1, Index: 1, Value: []byte("value 1")}, *entry)

	entry, err = storage.GetEntryAtIndex(2)
	assert.NoError(t, err)
	assert.Equal(t, types.Entry{Term: 2, Type: 2, Index: 2, Value: []byte("value 2")}, *entry)
}

func TestTruncateLogStartingFrom(t *testing.T) {
	t.Parallel()

	t.Run("truncate log", rapid.MakeCheck(func(t *rapid.T) {
		storage, err := NewFileStorage(fmt.Sprintf("%s/raft-go/%s", os.TempDir(), uuid.New().String()))
		assert.NoError(t, err)

		entries := rapid.SliceOf(entryGenerator()).
			Filter(func(entries []types.Entry) bool { return len(entries) > 0 }).
			Draw(t, "entries")

		assert.NoError(t, storage.AppendEntries(entries))

		index := rapid.Uint64Range(1, uint64(len(entries))).Draw(t, "index")

		assert.NoError(t, storage.TruncateLogStartingFrom(index))

		// Try to get index that doesn't exist because the log has been truncated starting from the index.
		_, err = storage.GetEntryAtIndex(index)
		assert.ErrorIs(t, err, ErrIndexOutOfBounds)

		// The log was truncated starting from the first entry.
		if index == 1 {
			return
		}

		// Ensure entry at previous index still exists.
		// -1 because the slice is 0-based and the storage is 1-based.
		// -1 because we want the previous index.
		expected := entries[index-2]
		expected.Index = index - 1

		entry, err := storage.GetEntryAtIndex(index - 1)
		assert.NoError(t, err)
		assert.Equal(t, expected, *entry)
	}))

	t.Run("from beginning", func(t *testing.T) {
		t.Parallel()

		storage, err := NewFileStorage(fmt.Sprintf("%s/raft-go/%s", os.TempDir(), uuid.New().String()))
		assert.NoError(t, err)

		assert.NoError(t, storage.AppendEntries([]types.Entry{
			{
				Term:  1,
				Type:  1,
				Value: []byte("value 1"),
			},
			{
				Term:  2,
				Type:  2,
				Value: []byte("value 2"),
			},
		}))

		assert.NoError(t, storage.TruncateLogStartingFrom(1))

		_, err = storage.GetEntryAtIndex(1)
		assert.ErrorIs(t, err, ErrIndexOutOfBounds)
	})

	t.Run("from the middle", func(t *testing.T) {
		t.Parallel()

		storage, err := NewFileStorage(fmt.Sprintf("%s/raft-go/%s", os.TempDir(), uuid.New().String()))
		assert.NoError(t, err)

		assert.NoError(t, storage.AppendEntries([]types.Entry{
			{
				Term:  1,
				Type:  1,
				Value: []byte("value 1"),
			},
			{
				Term:  2,
				Type:  2,
				Value: []byte("value 2"),
			},
			{
				Term:  3,
				Type:  3,
				Value: []byte("value 3"),
			},
		}))

		assert.NoError(t, storage.TruncateLogStartingFrom(2))

		_, err = storage.GetEntryAtIndex(2)
		assert.ErrorIs(t, err, ErrIndexOutOfBounds)
	})

	t.Run("from the end", func(t *testing.T) {
		t.Parallel()

		storage, err := NewFileStorage(fmt.Sprintf("%s/raft-go/%s", os.TempDir(), uuid.New().String()))
		assert.NoError(t, err)

		assert.NoError(t, storage.AppendEntries([]types.Entry{
			{
				Term:  1,
				Type:  1,
				Value: []byte("value 1"),
			},
			{
				Term:  2,
				Type:  2,
				Value: []byte("value 2"),
			},
		}))

		assert.NoError(t, storage.TruncateLogStartingFrom(2))

		entry, err := storage.GetEntryAtIndex(1)
		assert.NoError(t, err)
		assert.Equal(t, types.Entry{Term: 1, Index: 1, Type: 1, Value: []byte("value 1")}, *entry)
	})
}

func TestGetEntryAtIndex(t *testing.T) {
	t.Parallel()

	t.Run("returns entry at index", rapid.MakeCheck(func(t *rapid.T) {
		storage, err := NewFileStorage(fmt.Sprintf("%s/raft-go/%s", os.TempDir(), uuid.New().String()))
		assert.NoError(t, err)

		entries := rapid.SliceOf(entryGenerator()).
			Filter(func(entries []types.Entry) bool {
				return len(entries) > 0
			}).
			Draw(t, "entries")

		assert.NoError(t, storage.AppendEntries(entries))

		index := rapid.Uint64Range(1, uint64(len(entries))).Draw(t, "index")

		entry, err := storage.GetEntryAtIndex(index)
		assert.NoError(t, err)

		expected := entries[index-1]
		expected.Index = index

		assert.Equal(t, expected, *entry)
	}))

	t.Run("returns out of bounds for index that are greater than the last index", rapid.MakeCheck(func(t *rapid.T) {
		storage, err := NewFileStorage(fmt.Sprintf("%s/raft-go/%s", os.TempDir(), uuid.New().String()))
		assert.NoError(t, err)

		entries := rapid.SliceOf(entryGenerator()).Draw(t, "entries")

		assert.NoError(t, storage.AppendEntries(entries))

		_, err = storage.GetEntryAtIndex(uint64(len(entries) + 1))
		assert.ErrorIs(t, err, ErrIndexOutOfBounds)
	}))
}

func entryGenerator() *rapid.Generator[types.Entry] {
	return rapid.Custom(func(t *rapid.T) types.Entry {
		return types.Entry{
			Term:  rapid.Uint64().Draw(t, "Term"),
			Type:  rapid.Uint8().Draw(t, "Type"),
			Value: rapid.SliceOf(rapid.Byte()).Draw(t, "Value"),
		}
	})
}
