package raft

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
)

const kvSetCommand = 2

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
			LeaderID: leader.Config.ReplicaID,
			// And the leader is at term 1.
			LeaderTerm:        1,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries:           make([]types.Entry, 0),
		})

		env.Network.Tick()

		assert.NoError(t, replica.handleMessages(context.Background()))

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

	t.Run("leader message previous log index is not the same as the replicas last log index, success=false", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		leader := env.Replicas[0]
		replica := env.Replicas[1]

		assert.NoError(t, leader.newTerm(withTerm(1)))

		appendEntriesInput := types.AppendEntriesInput{
			LeaderID:          leader.Config.ReplicaID,
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

	t.Run("replica has entry with a different term at index, should truncate replica's log", func(t *testing.T) {
		t.Parallel()

		// TODO
	})

	t.Run("appends entries to the log, success=true", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		leader := env.Replicas[0]
		replica := env.Replicas[1]

		env.Bus.SendAppendEntriesRequest(leader.ReplicaAddress(), replica.ReplicaAddress(), types.AppendEntriesInput{
			LeaderID:          leader.Config.ReplicaID,
			LeaderTerm:        2,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries: []types.Entry{
				{
					Term: 2,
				},
			},
		})

		env.Network.Tick()

		assert.NoError(t, replica.handleMessages(context.Background()))

		env.Network.Tick()

		message, err := env.Bus.Receive(leader.ReplicaAddress())
		assert.NoError(t, err)

		response := message.(*types.AppendEntriesOutput)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
		assert.True(t, response.Success)

		entry, err := replica.Storage.GetEntryAtIndex(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), entry.Term)
	})

	t.Run("leader commit index is greater than the replica commit index, should apply entries to state machine", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		leader := env.Replicas[0]
		replica := env.Replicas[1]

		assert.NoError(t, leader.newTerm(withTerm(1)))

		entryValue, err := json.Marshal(map[string]any{
			"key":   "key1",
			"value": []byte("value1"),
		})
		assert.NoError(t, err)

		env.Bus.SendAppendEntriesRequest(leader.ReplicaAddress(), replica.ReplicaAddress(), types.AppendEntriesInput{
			LeaderID:          leader.Config.ReplicaID,
			LeaderTerm:        leader.mutableState.currentTermState.term,
			LeaderCommitIndex: leader.mutableState.commitIndex,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries: []types.Entry{
				{
					Term:  leader.mutableState.currentTermState.term,
					Type:  kvSetCommand,
					Value: entryValue,
				},
			},
		})

		env.Network.Tick()

		replica.Tick()

		env.Network.Tick()

		assert.NoError(t, leader.newTerm(withTerm(2)))
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
			LeaderID:          leader.Config.ReplicaID,
			LeaderTerm:        leader.mutableState.currentTermState.term,
			LeaderCommitIndex: leader.mutableState.commitIndex,
			PreviousLogIndex:  1,
			PreviousLogTerm:   leader.mutableState.currentTermState.term - 1,
			Entries: []types.Entry{
				{
					Term:  leader.mutableState.currentTermState.term,
					Type:  kvSetCommand,
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
