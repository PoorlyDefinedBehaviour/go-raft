package raft

import (
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/slicesx"
	"github.com/stretchr/testify/assert"
)

func TestLeaderElectionTimeoutFired(t *testing.T) {
	t.Parallel()

	cluster := Setup()

	replica := cluster.Replicas[0]

	// Tick until the timeout fires.
	for i := 0; i < int(replica.mutableState.nextLeaderElectionTimeout); i++ {
		replica.Tick()
	}

	assert.True(t, replica.leaderElectionTimeoutFired())
}

func TestLeaderElection(t *testing.T) {
	t.Parallel()

	t.Run("every new leader commits an empty entry to reset replica leader election timeouts", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Replicas[0]

		assert.NoError(t, candidate.transitionToState(Candidate))

		replicas := cluster.Replicas[1:]

		originalTimeouts := slicesx.Map(replicas, func(replica *TestReplica) uint64 {
			return replica.mutableState.nextLeaderElectionTimeout
		})

		assert.NoError(t, candidate.startElection())

		actualLeader := cluster.MustWaitForLeader()

		assert.Equal(t, candidate, actualLeader)

		cluster.Tick()

		timeoutsAfterLeaderHeartbeat := slicesx.Map(replicas, func(replica *TestReplica) uint64 {
			return replica.mutableState.nextLeaderElectionTimeout
		})

		for i := range originalTimeouts {
			assert.True(t, originalTimeouts[i] < timeoutsAfterLeaderHeartbeat[i])
		}
	})
}
