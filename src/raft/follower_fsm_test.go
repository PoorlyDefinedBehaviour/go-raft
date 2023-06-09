package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFollowerFSM(t *testing.T) {
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
}
