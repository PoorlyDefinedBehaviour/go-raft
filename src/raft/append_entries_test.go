package raft

import (
	"encoding/json"
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
)

func TestHandleMessagesAppendEntriesRequest(t *testing.T) {
	t.Parallel()

	t.Run("leader term is smaller than the replica term, success=false", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		leader := env.Replicas[0]
		replica := env.Replicas[1]

		// This replica is at term 2.
		assert.NoError(t, replica.newTerm(withTerm(2)))

		env.Bus.SendAppendEntriesRequest(leader.ReplicaAddress(), replica.ReplicaAddress(), types.AppendEntriesInput{
			LeaderID: leader.config.ReplicaID,
			// And the leader is at term 1.
			LeaderTerm:        1,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries:           make([]types.Entry, 0),
		})

		env.Network.Tick()

		replica.handleMessages()

		env.Network.Tick()

		// The leader should receive a unsuccessful response because its term is lower than the replica's term.
		message, err := env.Bus.Receive(leader.ReplicaAddress())
		assert.NoError(t, err)

		response := message.(*types.AppendEntriesOutput)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
		assert.False(t, response.Success)
		assert.Equal(t, uint64(0), response.PreviousLogIndex)
		assert.Equal(t, uint64(0), response.PreviousLogTerm)
	})

	t.Run("leader message previous log index indes is not the same as the replicas last log index, success=false", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		leader := env.Replicas[0]
		replica := env.Replicas[1]

		leader.newTerm(withTerm(1))

		appendEntriesInput := types.AppendEntriesInput{
			LeaderID:          leader.config.ReplicaID,
			LeaderTerm:        leader.mutableState.currentTermState.term,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  1,
			PreviousLogTerm:   1,
			Entries:           make([]types.Entry, 0),
		}

		env.Bus.SendAppendEntriesRequest(leader.ReplicaAddress(), replica.ReplicaAddress(), appendEntriesInput)

		env.Network.Tick()

		replica.Tick()

		env.Network.Tick()

		message, err := env.Bus.Receive(leader.ReplicaAddress())
		assert.NoError(t, err)

		response := message.(*types.AppendEntriesOutput)

		assert.False(t, response.Success)
		assert.Equal(t, uint64(0), response.PreviousLogIndex)
	})

	t.Run("replica entry conflicts with leader entry in the same index, should truncate the replica log", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		leader := env.Replicas[0]
		replica := env.Replicas[1]

		// The last log entry is at term 0 in the replica.
		assert.NoError(t, env.Storage.AppendEntries([]types.Entry{
			{
				Term: 0,
			},
			{
				Term: 0,
			},
		}))

		env.Bus.SendAppendEntriesRequest(leader.ReplicaAddress(), replica.ReplicaAddress(), types.AppendEntriesInput{
			LeaderID:          leader.config.ReplicaID,
			LeaderTerm:        2,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  2,
			// The leader thinks the last log is at term 1.
			PreviousLogTerm: 1,
			Entries: []types.Entry{
				{
					Term: 2,
				},
			},
		})

		env.Network.Tick()

		replica.handleMessages()

		entry, err := replica.storage.GetEntryAtIndex(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), entry.Term)

		entry, err = replica.storage.GetEntryAtIndex(2)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), entry.Term)

		_, err = replica.storage.GetEntryAtIndex(3)
		assert.Contains(t, err.Error(), "index out of bounds")
	})

	t.Run("appends entries to the log, success=true", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		leader := env.Replicas[0]
		replica := env.Replicas[1]

		env.Bus.SendAppendEntriesRequest(leader.ReplicaAddress(), replica.ReplicaAddress(), types.AppendEntriesInput{
			LeaderID:          leader.config.ReplicaID,
			LeaderTerm:        0,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries: []types.Entry{
				{
					Term: 0,
				},
			},
		})

		env.Network.Tick()

		replica.handleMessages()

		env.Network.Tick()

		message, err := env.Bus.Receive(leader.ReplicaAddress())
		assert.NoError(t, err)

		response := message.(*types.AppendEntriesOutput)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
		assert.True(t, response.Success)

		entry, err := replica.storage.GetEntryAtIndex(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), entry.Term)
	})

	t.Run("leader commit index is greater than the replica commit index, should apply entries to state machine", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		leader := env.Replicas[0]
		replica := env.Replicas[1]

		leader.newTerm(withTerm(1))

		entryValue, err := json.Marshal(map[string]any{
			"key":   "key1",
			"value": []byte("value1"),
		})
		assert.NoError(t, err)

		env.Bus.SendAppendEntriesRequest(leader.ReplicaAddress(), replica.ReplicaAddress(), types.AppendEntriesInput{
			LeaderID:          leader.config.ReplicaID,
			LeaderTerm:        leader.mutableState.currentTermState.term,
			LeaderCommitIndex: leader.mutableState.commitIndex,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries: []types.Entry{
				{
					Term:  leader.mutableState.currentTermState.term,
					Type:  kv.SetCommand,
					Value: entryValue,
				},
			},
		})

		env.Network.Tick()

		replica.Tick()

		env.Network.Tick()

		leader.newTerm(withTerm(2))
		leader.mutableState.commitIndex = 1

		message, err := env.Bus.Receive(leader.ReplicaAddress())
		assert.NoError(t, err)
		response := message.(*types.AppendEntriesOutput)
		assert.True(t, response.Success)

		entryValue, err = json.Marshal(map[string]string{
			"key":   "key2",
			"value": "value2",
		})
		assert.NoError(t, err)

		env.Bus.SendAppendEntriesRequest(leader.ReplicaAddress(), replica.ReplicaAddress(), types.AppendEntriesInput{
			LeaderID:          leader.config.ReplicaID,
			LeaderTerm:        leader.mutableState.currentTermState.term,
			LeaderCommitIndex: leader.mutableState.commitIndex,
			PreviousLogIndex:  1,
			PreviousLogTerm:   leader.mutableState.currentTermState.term - 1,
			Entries: []types.Entry{
				{
					Term:  leader.mutableState.currentTermState.term,
					Type:  kv.SetCommand,
					Value: entryValue,
				},
			},
		})

		env.Network.Tick()

		replica.Tick()

		env.Network.Tick()

		message, err = env.Bus.Receive(leader.ReplicaAddress())
		assert.NoError(t, err)
		response = message.(*types.AppendEntriesOutput)
		assert.True(t, response.Success)

		// First entry has been applied.
		value, ok := replica.Kv.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, []byte("value1"), value)

		// Second entry has not been applied yet.
		_, ok = replica.Kv.Get("key2")
		assert.False(t, ok)
	})
}
