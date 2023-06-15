package raft

import (
	"encoding/json"
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
)

func TestSendHeartbeat(t *testing.T) {
	t.Parallel()

	panic("todo")
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

			cluster.Bus.RequestVote(candidate.ReplicaAddress(), leader.ReplicaAddress(), types.RequestVoteInput{
				CandidateID:           candidate.config.ReplicaID,
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

	t.Run("leader applies entry to fsm and commits after replicating to majority replicas", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		leader := cluster.MustWaitForLeader()

		value, err := json.Marshal(map[string]any{
			"key":   "key",
			"value": []byte("value"),
		})
		assert.NoError(t, err)

		request, err := leader.HandleUserRequest(kv.SetCommand, value)
		assert.NoError(t, err)

		// Send messages to replicas
		cluster.Tick()

		err = <-request.DoneCh
		assert.NoError(t, err)

		value, ok := leader.Kv.Get("key")
		assert.True(t, ok)
		assert.Equal(t, []byte("value"), value)
	})

	t.Run("leader keeps track of the next log index that will be sent to replicas", func(t *testing.T) {
		t.Parallel()

		t.Run("empty heartbeat", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			leader := cluster.Replicas[0]

			// Transitioning to leader appends an empty entry to the log.
			assert.NoError(t, leader.transitionToState(Leader))

			// Will send the empty entry to replicas.
			assert.NoError(t, leader.sendHeartbeat())

			// Sent one entry to each replica, next entry index starts at 2.
			for _, replica := range cluster.Followers() {
				assert.Equal(t, uint64(2), leader.mutableState.nextIndex[replica.config.ReplicaID])
			}
		})

		t.Run("non-empty heartbeat", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			leader := cluster.Replicas[0]

			// Transitioning to leader appends one empty entry to the logl.
			assert.NoError(t, leader.transitionToState(Leader))

			// Leader appended another entry to the log.
			assert.NoError(t, leader.storage.AppendEntries([]types.Entry{
				{
					Term:  leader.mutableState.currentTermState.term,
					Type:  1,
					Value: []byte("hello world"),
				},
			},
			))

			// Will send entries at index 1 and 2 to replicas.
			assert.NoError(t, leader.sendHeartbeat())

			// Next entry starts at index 3.
			for _, replica := range cluster.Followers() {
				assert.Equal(t, uint64(3), leader.mutableState.nextIndex[replica.config.ReplicaID])
			}
		})
	})
}
