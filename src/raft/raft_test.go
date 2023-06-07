package raft

import (
	"testing"
	"time"

	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"

	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	"github.com/stretchr/testify/assert"

	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	"github.com/poorlydefinedbehaviour/raft-go/src/testing/network"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type SetupOutput struct {
	Replicas []TestReplica
	Network  *network.Network
	Storage  storage.Storage
	Bus      *messagebus.MessageBus
}

type TestReplica struct {
	*Raft
	Kv *kv.KvStore[string, string]
}

func setup() SetupOutput {
	replicaAddresses := []types.ReplicaAddress{"localhost:8001", "localhost:8002", "localhost:8003"}

	network := network.NewNetwork(network.NetworkConfig{
		PathClogProbability:      0.0,
		MessageReplayProbability: 0.0,
		DropMessageProbability:   0.0,
		MaxNetworkPathClogTicks:  0,
		MaxMessageDelayTicks:     0,
	},
		rand.NewRand(0),
		replicaAddresses,
	)
	bus := messagebus.NewMessageBus(network)

	storage := storage.NewFileStorage()

	replicas := make([]TestReplica, 0)

	for i, replicaAddress := range replicaAddresses {
		configReplicas := make([]Replica, 0)

		for j, otherReplicaAddress := range replicaAddresses {
			if replicaAddress == otherReplicaAddress {
				continue
			}
			configReplicas = append(configReplicas, Replica{ReplicaID: uint16(j + 1), ReplicaAddress: otherReplicaAddress})
		}

		config := Config{
			ReplicaID:             uint16(i + 1),
			ReplicaAddress:        replicaAddress,
			Replicas:              configReplicas,
			LeaderElectionTimeout: 10 * time.Second,
		}
		kv := kv.NewKvStore[string, string]()
		raft, err := NewRaft(config, bus, storage, kv)
		if err != nil {
			panic(err)
		}
		replicas = append(replicas, TestReplica{Raft: raft, Kv: kv})
	}

	return SetupOutput{Replicas: replicas, Network: network, Bus: bus, Storage: storage}
}

func TestLeaderElectionTimeoutFired(t *testing.T) {
	cases := []struct {
		description        string
		config             Config
		initialCurrentTick uint64
		expected           bool
	}{
		{
			description:        "leader election timeout is set to a tick greater than the current tick, should return false",
			config:             Config{ReplicaID: 1, ReplicaAddress: "localhost:8001", LeaderElectionTimeout: 5 * time.Second},
			initialCurrentTick: 0,
			expected:           false,
		},
		{
			description:        "leader election timeout is set to a tick smaller than the current tick, should return true",
			config:             Config{ReplicaID: 1, ReplicaAddress: "localhost:8001", LeaderElectionTimeout: 5 * time.Second},
			initialCurrentTick: 5001,
			expected:           true,
		},
	}

	for _, tt := range cases {
		raft, err := NewRaft(tt.config, messagebus.NewMessageBus(nil), nil, nil)
		assert.NoError(t, err)
		raft.mutableState.currentTick = tt.initialCurrentTick
		actual := raft.leaderElectionTimeoutFired()
		assert.Equal(t, tt.expected, actual, tt.description)
	}
}
