package raft

import (
	"testing"
	"time"

	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/slicesx"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestLeaderElectionTimeoutFired(t *testing.T) {
	t.Parallel()

	log, err := zap.NewProduction()
	assert.NoError(t, err)
	logger := log.Sugar()

	cases := []struct {
		description        string
		config             Config
		initialCurrentTick uint64
		expected           bool
	}{
		{
			description: "leader election timeout is set to a tick greater than the current tick, should return false",
			config: Config{
				ReplicaID:              1,
				ReplicaAddress:         "localhost:8001",
				LeaderElectionTimeout:  300 * time.Millisecond,
				LeaderHeartbeatTimeout: 100 * time.Millisecond,
			},
			initialCurrentTick: 0,
			expected:           false,
		},
		{
			description: "leader election timeout is set to a tick smaller than the current tick, should return true",
			config: Config{
				ReplicaID:              1,
				ReplicaAddress:         "localhost:8001",
				LeaderElectionTimeout:  300 * time.Millisecond,
				LeaderHeartbeatTimeout: 100 * time.Millisecond,
			},
			initialCurrentTick: 5001,
			expected:           true,
		},
	}

	for _, tt := range cases {
		raft, err := NewRaft(tt.config, messagebus.NewMessageBus(nil), nil, nil, rand.NewRand(0), logger)
		assert.NoError(t, err)
		raft.mutableState.currentTick = tt.initialCurrentTick
		actual := raft.leaderElectionTimeoutFired()
		assert.Equal(t, tt.expected, actual, tt.description)
	}
}

func TestLeaderElection(t *testing.T) {
	t.Parallel()

	t.Run("only one leader is elected per term", func(t *testing.T) {
		t.Parallel()

		for i := 0; i < 100; i++ {
			cluster := Setup()

			replica := cluster.Replicas[0]

			for i := 0; i < int(replica.config.LeaderElectionTimeout.Milliseconds())+100; i++ {
				cluster.Tick()
			}

			leaders := 0
			for _, replica := range cluster.Replicas {
				if replica.State() == Leader {
					leaders++
				}
			}

			assert.Equal(t, 1, leaders)
		}
	})

	t.Run("every new leader commits an empty entry to reset replica leader election timeouts", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Replicas[0]

		replicas := cluster.Replicas[1:]

		originalTimeouts := slicesx.Map(replicas, func(replica *TestReplica) uint64 {
			return replica.mutableState.nextLeaderElectionTimeout
		})

		candidate.startElection()

		actualLeader := cluster.MustWaitForLeader()

		assert.Equal(t, candidate, actualLeader)

		timeoutsAfterLeaderHeartbeat := slicesx.Map(replicas, func(replica *TestReplica) uint64 {
			return replica.mutableState.nextLeaderElectionTimeout
		})

		for i := range originalTimeouts {
			assert.True(t, originalTimeouts[i] < timeoutsAfterLeaderHeartbeat[i])
		}
	})
}
