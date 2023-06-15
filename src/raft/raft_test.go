package raft

import (
	"testing"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"go.uber.org/zap"

	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	"github.com/poorlydefinedbehaviour/raft-go/src/testing/network"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type Cluster struct {
	Replicas []TestReplica
	Network  *network.Network
	Bus      *messagebus.MessageBus
}

type TestReplica struct {
	*Raft
	Kv *kv.KvStore
}

func (cluster *Cluster) Followers() []TestReplica {
	replicas := make([]TestReplica, 0)

	for _, replica := range cluster.Replicas {
		if replica.State() != Leader {
			replicas = append(replicas, replica)
		}
	}

	return replicas
}

func (cluster *Cluster) MustWaitForCandidate() TestReplica {
	return cluster.mustWaitForReplicaWithStatus(Candidate)
}

func (cluster *Cluster) MustWaitForLeader() TestReplica {
	return cluster.mustWaitForReplicaWithStatus(Leader)
}

func (cluster *Cluster) mustWaitForReplicaWithStatus(state State) TestReplica {
	const maxTicks = 350

	for i := 0; i < maxTicks; i++ {
		cluster.Tick()

		for _, replica := range cluster.Replicas {
			if replica.State() == state {
				return replica
			}
		}
	}

	panic("unable to elect a leader in time")
}

func (cluster *Cluster) Tick() {
	cluster.Bus.Tick()
	cluster.Network.Tick()

	for _, replica := range cluster.Replicas {
		replica.Raft.Tick()
	}
}

func (cluster *Cluster) Leader() *TestReplica {
	for _, replica := range cluster.Replicas {
		if replica.Raft.State() == Leader {
			return &replica
		}
	}

	return nil
}

func Setup() Cluster {
	log, err := zap.NewDevelopment(zap.WithCaller(true))
	if err != nil {
		panic(err)
	}
	logger := log.Sugar()

	replicaAddresses := []types.ReplicaAddress{"localhost:8001", "localhost:8002", "localhost:8003"}

	rand := rand.NewRand(0)

	network := network.NewNetwork(network.NetworkConfig{
		PathClogProbability:      0.0,
		MessageReplayProbability: 0.0,
		DropMessageProbability:   0.0,
		MaxNetworkPathClogTicks:  0,
		MaxMessageDelayTicks:     0,
	},
		logger,
		rand,
		replicaAddresses,
	)
	bus := messagebus.NewMessageBus(network)

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
			ReplicaID:                uint16(i + 1),
			ReplicaAddress:           replicaAddress,
			Replicas:                 configReplicas,
			MaxLeaderElectionTimeout: 300 * time.Millisecond,
			MinLeaderElectionTimeout: 100 * time.Millisecond,
			LeaderHeartbeatTimeout:   100 * time.Millisecond,
		}

		kv := kv.NewKvStore(bus)
		raft, err := NewRaft(config, bus, storage.NewFileStorage(), kv, rand, logger)
		if err != nil {
			panic(err)
		}
		replicas = append(replicas, TestReplica{Raft: raft, Kv: kv})
	}

	return Cluster{Replicas: replicas, Network: network, Bus: bus}
}

func TestApplyCommittedEntries(t *testing.T) {
	t.Parallel()

	panic("todo")
}

func TestHandleUserRequest(t *testing.T) {
	t.Parallel()

	panic("todo")
}

func TestVoteFor(t *testing.T) {
	t.Parallel()

	panic("todo")
}
