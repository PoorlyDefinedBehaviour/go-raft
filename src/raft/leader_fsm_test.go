package raft

import (
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
)

func TestLeaderFSM(t *testing.T) {
	t.Parallel()

	t.Run("leader transitions to follower when its term is out of date", func(t *testing.T) {
		t.Parallel()

		t.Run("append entries request", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			oldLeader := cluster.Replicas[0]
			oldLeader.transitionToState(Leader)

			newLeader := cluster.Replicas[1]
			newLeader.transitionToState(Leader)

			newLeader.newTerm(withTerm(oldLeader.mutableState.currentTermState.term))

			cluster.Bus.SendAppendEntriesRequest(newLeader.ReplicaAddress(), oldLeader.ReplicaAddress(), types.AppendEntriesInput{
				LeaderID:          newLeader.config.ReplicaID,
				LeaderTerm:        newLeader.mutableState.currentTermState.term,
				LeaderCommitIndex: 0,
				PreviousLogIndex:  0,
				PreviousLogTerm:   0,
				Entries:           make([]types.Entry, 0),
			})

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
			leader.transitionToState(Leader)

			candidate := cluster.Replicas[1]
			candidate.transitionToState(Candidate)

			candidate.newTerm(withTerm(leader.mutableState.currentTermState.term + 1))

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

		panic("todo")
	})

	t.Run("leader keeps track of the next log index that will be sent to replicas", func(t *testing.T) {
		t.Parallel()

		t.Run("empty heartbeat", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			leader := cluster.Replicas[0]
			leader.transitionToState(Leader)

			// No entries to send, next index shouldn't change.
			leader.sendHeartbeat()

			for _, replica := range cluster.Followers() {
				assert.Equal(t, uint64(1), leader.mutableState.nextIndex[replica.config.ReplicaID])
			}
		})

		t.Run("non-empty heartbeat", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			leader := cluster.Replicas[0]
			leader.transitionToState(Leader)

			leader.storage.AppendEntries([]types.Entry{
				{
					Term:  leader.mutableState.currentTermState.term,
					Type:  1,
					Value: []byte("hello world"),
				},
			},
			)

			leader.sendHeartbeat()

			for _, replica := range cluster.Followers() {
				assert.Equal(t, uint64(2), leader.mutableState.nextIndex[replica.config.ReplicaID])
			}
		})
	})
}
