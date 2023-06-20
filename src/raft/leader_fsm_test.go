package raft

import (
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
)

func TestSendHeartbeat(t *testing.T) {
	t.Parallel()

	// TODO
}

func TestLeaderFSM(t *testing.T) {
	t.Parallel()

	t.Run("leader transitions to follower when its term is out of date", func(t *testing.T) {
		t.Parallel()

		t.Run("new leader append entries request", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			oldLeader := cluster.Replicas[0]
			assert.NoError(t, oldLeader.transitionToState(Leader))

			newLeader := cluster.Replicas[1]
			assert.NoError(t, newLeader.newTerm(withTerm(oldLeader.mutableState.currentTermState.term+1)))
			assert.NoError(t, newLeader.transitionToState(Leader))
			assert.NoError(t, newLeader.sendHeartbeat())

			cluster.Bus.Tick()
			cluster.Network.Tick()
			oldLeader.Tick()

			assert.Equal(t, Follower, oldLeader.State())
			assert.Equal(t, newLeader.mutableState.currentTermState.term, oldLeader.mutableState.currentTermState.term)
		})

		t.Run("request vote request", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			leader := cluster.Replicas[0]
			assert.NoError(t, leader.transitionToState(Leader))

			candidate := cluster.Replicas[1]
			assert.NoError(t, candidate.transitionToState(Candidate))

			assert.NoError(t, candidate.newTerm(withTerm(leader.mutableState.currentTermState.term+1)))

			cluster.Bus.Send(candidate.ReplicaAddress(), leader.ReplicaAddress(), &types.RequestVoteInput{
				CandidateID:           candidate.Config.ReplicaID,
				CandidateTerm:         candidate.mutableState.currentTermState.term,
				CandidateLastLogIndex: 0,
				CandidateLastLogTerm:  0,
			})

			cluster.Bus.Tick()
			cluster.Network.Tick()

			leader.Tick()

			assert.Equal(t, Follower, leader.State())
			assert.Equal(t, candidate.mutableState.currentTermState.term, leader.mutableState.currentTermState.term)
		})
	})

	t.Run("leader sends heartbeat to followers", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		leader := cluster.MustWaitForLeader()

		// Tick until the timeout fires.
		heartbeatTimeoutAtTick := leader.mutableState.nextLeaderHeartbeatTimeout + 1
		for i := uint64(0); i < heartbeatTimeoutAtTick; i++ {
			leader.Tick()
		}

		expected := &types.AppendEntriesInput{
			LeaderID:          leader.Config.ReplicaID,
			LeaderTerm:        leader.mutableState.currentTermState.term,
			LeaderCommitIndex: leader.mutableState.commitIndex,
			PreviousLogIndex:  leader.Storage.LastLogIndex() - 1,
			PreviousLogTerm:   leader.Storage.LastLogTerm() - 1,
			Entries: []types.Entry{
				{
					Term:  leader.Term(),
					Type:  types.NewLeaderEntryType,
					Value: nil,
				},
			},
		}

		// Ensure leader sent heartbeat to followers.
		for _, replica := range cluster.Followers() {
			messages := cluster.Network.MessagesFromTo(leader.ReplicaAddress(), replica.ReplicaAddress())

			// 1 message because of the heartbeat sent by the newly elected leader.
			// 1 message sent because of the heartbeat after the heartbeat timeout fired.
			assert.Equal(t, 2, len(messages))

			for _, message := range messages {
				assert.Equal(t, expected, message)
			}
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

	t.Run("leader keeps track of the next log index that will be sent to replicas", func(t *testing.T) {
		t.Parallel()

		t.Run("empty heartbeat", func(t *testing.T) {
			t.Parallel()

			// TODO
		})

		t.Run("non-empty heartbeat", func(t *testing.T) {
			t.Parallel()

			// TODO
		})
	})
}
