package testingcluster

import (
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/raft"
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	"github.com/poorlydefinedbehaviour/raft-go/src/testing/network"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"go.uber.org/zap"
)

type Cluster struct {
	Replicas []TestReplica
	Network  *network.Network
	Bus      *messagebus.MessageBus
}

type TestReplica struct {
	*raft.Raft
	Kv *kv.KvStore
}

func (cluster *Cluster) Followers() []TestReplica {
	replicas := make([]TestReplica, 0)

	for _, replica := range cluster.Replicas {
		if replica.State() != raft.Leader {
			replicas = append(replicas, replica)
		}
	}

	return replicas
}

func (cluster *Cluster) MustWaitForCandidate() TestReplica {
	return cluster.mustWaitForReplicaWithStatus(raft.Candidate)
}

func (cluster *Cluster) MustWaitForLeader() TestReplica {
	return cluster.mustWaitForReplicaWithStatus(raft.Leader)
}

func (cluster *Cluster) mustWaitForReplicaWithStatus(state raft.State) TestReplica {
	const maxTicks = 10_000

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

func (cluster *Cluster) Start() {

	for {
		cluster.Tick()
		time.Sleep(1 * time.Millisecond)
	}
}

func (cluster *Cluster) TickUntilEveryMessageIsDelivered() {
	for cluster.Network.HasPendingMessages() {
		cluster.Tick()
	}
}

func (cluster *Cluster) Tick() {
	cluster.Bus.Tick()
	cluster.Network.Tick()

	for _, replica := range cluster.Replicas {
		replica.Tick()
	}
}

func (cluster *Cluster) Leader() *TestReplica {
	for _, replica := range cluster.Replicas {
		if replica.Raft.State() == raft.Leader {
			return &replica
		}
	}

	return nil
}

func Setup() Cluster {
	log, err := zap.NewProduction(zap.WithCaller(true))
	if err != nil {
		panic(err)
	}
	logger := log.Sugar()

	rand := rand.NewRand(0)

	replicaAddresses := []types.ReplicaAddress{"localhost:8001", "localhost:8002", "localhost:8003"}

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
		configReplicas := make([]raft.Replica, 0)

		for j, otherReplicaAddress := range replicaAddresses {
			if replicaAddress == otherReplicaAddress {
				continue
			}
			configReplicas = append(configReplicas, raft.Replica{ReplicaID: uint16(j + 1), ReplicaAddress: otherReplicaAddress})
		}

		config := raft.Config{
			ReplicaID:                uint16(i + 1),
			ReplicaAddress:           replicaAddress,
			Replicas:                 configReplicas,
			MaxLeaderElectionTimeout: 300 * time.Millisecond,
			MinLeaderElectionTimeout: 100 * time.Millisecond,
			LeaderHeartbeatTimeout:   100 * time.Millisecond,
		}

		kv := kv.NewKvStore(bus)
		raft, err := raft.NewRaft(config, bus, storage.NewFileStorage(), kv, rand, logger)
		if err != nil {
			panic(err)
		}
		replicas = append(replicas, TestReplica{Raft: raft, Kv: kv})
	}

	return Cluster{Replicas: replicas, Network: network, Bus: bus}
}
