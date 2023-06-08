package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCandidateFSM(t *testing.T) {
	t.Parallel()

	t.Run("follower transitions to candidate when leader election timeout fires", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Followers()[0]

		// Tick until the timeout fires.
		timeoutAtTick := replica.mutableState.nextLeaderElectionTimeout + 1
		for i := uint64(0); i < timeoutAtTick; i++ {
			replica.Tick()
		}

		assert.Equal(t, Candidate, replica.State())
	})

	t.Run("leader election timeout is reset when follower transitions to candidate", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Followers()[0]

		// Tick until the timeout fires.
		timeoutAtTick := replica.mutableState.nextLeaderElectionTimeout + 1
		for i := uint64(0); i < timeoutAtTick; i++ {
			replica.Tick()
		}
		// Tick one more time to enter the Candidate fsm.
		replica.Tick()

		assert.True(t, timeoutAtTick < replica.mutableState.nextLeaderElectionTimeout)
	})

	t.Run("candidate starts an election after transitioning from follower->candidate", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Followers()[0]

		termBeforeElection := replica.mutableState.currentTermState.term

		// Tick until the timeout fires.
		timeoutAtTick := replica.mutableState.nextLeaderElectionTimeout + 1
		for i := uint64(0); i < timeoutAtTick; i++ {
			replica.Tick()
		}
		// Tick one more time to enter the Candidate fsm.
		replica.Tick()

		t.Run("candidate starts a new term", func(t *testing.T) {
			assert.True(t, termBeforeElection < replica.mutableState.currentTermState.term)
		})

		t.Run("candidate votes for itself", func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, replica.config.ReplicaID, replica.mutableState.currentTermState.votedFor)
		})
	})

	t.Run("election timed out, candidate should restart election", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Followers()[0]

		t.Run("election timeout, follower->candidate, start election", func(t *testing.T) {
			termBeforeElection := replica.mutableState.currentTermState.term

			// Tick until the timeout fires.
			timeoutAtTick := replica.mutableState.nextLeaderElectionTimeout + 1
			for i := uint64(0); i < timeoutAtTick; i++ {
				replica.Tick()
			}
			// Tick one more time to enter the Candidate fsm.
			replica.Tick()

			// Term is incremented every new election.
			assert.True(t, termBeforeElection < replica.mutableState.nextLeaderElectionTimeout)
		})

		t.Run("current election timeout, starts new election", func(t *testing.T) {
			termBeforeElection := replica.mutableState.currentTermState.term

			// Tick until the timeout fires again.
			timeoutAtTick := replica.mutableState.nextLeaderElectionTimeout + 1
			for i := uint64(0); i < timeoutAtTick; i++ {
				replica.Tick()
			}

			// New election, new term.
			assert.True(t, termBeforeElection < replica.mutableState.nextLeaderElectionTimeout)
		})
	})
}
