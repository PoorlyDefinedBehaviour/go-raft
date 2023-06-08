package raft

import (
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
)

func TestLeaderFSM(t *testing.T) {
	t.Parallel()

	t.Run("leader sends heartbeat to followers", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		leader := cluster.MustWaitForLeader()

		// Tick until the timeout fires.
		heartbeatTimeoutAtTick := leader.mutableState.nextLeaderHeartbeatTimeout + 1
		for i := uint64(0); i < heartbeatTimeoutAtTick; i++ {
			leader.Tick()
		}

		cluster.Network.Tick()

		// Ensure leader sent heartbeat to followers.
		for _, replica := range cluster.Followers() {
			message, err := cluster.Bus.Receive(replica.ReplicaAddress())
			assert.NoError(t, err)

			response := message.(*types.AppendEntriesInput)

			assert.Equal(t, &types.AppendEntriesInput{
				LeaderID:          leader.config.ReplicaID,
				LeaderTerm:        leader.mutableState.currentTermState.term,
				LeaderCommitIndex: leader.mutableState.commitIndex,
				PreviousLogIndex:  leader.storage.LastLogIndex(),
				PreviousLogTerm:   leader.storage.LastLogTerm(),
				Entries:           make([]types.Entry, 0),
			},
				response,
			)
		}
	})

	t.Run("leader resets heartbeat timeout after it fires", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		leader := cluster.MustWaitForLeader()

		// Tick until the timeout fires.
		heartbeatTimeoutAtTick := leader.mutableState.nextLeaderHeartbeatTimeout + 1
		for i := uint64(0); i < heartbeatTimeoutAtTick; i++ {
			leader.Tick()
		}

		// The next heartbeat should fire at a future tick.
		assert.True(t, heartbeatTimeoutAtTick < leader.mutableState.nextLeaderHeartbeatTimeout)
	})
}
