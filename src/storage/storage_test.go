package storage

import (
	"os"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewFileStorage(t *testing.T) {
	t.Parallel()

	t.Run("creates state file", func(t *testing.T) {
		dir := path.Join(os.TempDir(), uuid.NewString())

		// Execute more than once to ensure idempotency.
		for i := 0; i < 2; i++ {
			_, err := NewFileStorage(dir)
			assert.NoError(t, err)

			stateFilePath := path.Join(dir, "raft.state")
			_, err = os.Stat(stateFilePath)
			assert.NoError(t, err)
		}
	})

	t.Run("there's no state to recover from disk", func(t *testing.T) {
		dir := path.Join(os.TempDir(), uuid.NewString())

		storage, err := NewFileStorage(dir)
		assert.NoError(t, err)

		assert.Nil(t, storage.state)
	})

	t.Run("there's state to recover from disk", func(t *testing.T) {
		dir := path.Join(os.TempDir(), uuid.NewString())

		storage, err := NewFileStorage(dir)
		assert.NoError(t, err)

		assert.Nil(t, storage.state)

		state := State{
			CurrentTerm: 1,
			VotedFor:    4,
		}
		assert.NoError(t, storage.Persist(state))

		storage, err = NewFileStorage(dir)
		assert.NoError(t, err)
		assert.Equal(t, state, *storage.state)
	})
}

func TestPersist(t *testing.T) {
	t.Parallel()

	dir := path.Join(os.TempDir(), uuid.NewString())

	storage, err := NewFileStorage(dir)
	assert.NoError(t, err)

	t.Run("term=1 votedfor=2", func(t *testing.T) {
		state := State{
			CurrentTerm: 1,
			VotedFor:    2,
		}
		assert.NoError(t, storage.Persist(state))
		// Write twice to ensure idempotency
		assert.NoError(t, storage.Persist(state))

		stateFromDisk, err := getState(storage.stateFile)
		assert.NoError(t, err)

		assert.Equal(t, state, *stateFromDisk)
	})

	t.Run("term=2 votedfor=1", func(t *testing.T) {
		state := State{
			CurrentTerm: 2,
			VotedFor:    1,
		}
		assert.NoError(t, storage.Persist(state))
		// Write twice to ensure idempotency
		assert.NoError(t, storage.Persist(state))

		stateFromDisk, err := getState(storage.stateFile)
		assert.NoError(t, err)

		assert.Equal(t, state, *stateFromDisk)
	})
}
