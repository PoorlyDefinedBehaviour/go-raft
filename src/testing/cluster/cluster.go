package cluster

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
	Storage  storage.Storage
	Bus      *messagebus.MessageBus
}

type TestReplica struct {
	*raft.Raft
	Kv *kv.KvStore
}

func (cluster *Cluster) MustWaitForLeader() *TestReplica {
	const maxTicks = 10_100

	for i := 0; i < maxTicks; i++ {
		cluster.Tick()
		if leader := cluster.Leader(); leader != nil {
			return leader
		}
	}

	panic("unable to elect a leader in time")
}

func (cluster *Cluster) Start() {
	for {
		select {
		case <-time.After(1 * time.Millisecond):
			cluster.Tick()
		}
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

	replicaAddresses := []types.ReplicaAddress{"localhost:8001", "localhost:8002", "localhost:8003"}

	network := network.NewNetwork(network.NetworkConfig{
		PathClogProbability:      0.0,
		MessageReplayProbability: 0.0,
		DropMessageProbability:   0.0,
		MaxNetworkPathClogTicks:  0,
		MaxMessageDelayTicks:     0,
	},
		logger,
		rand.NewRand(0),
		replicaAddresses,
	)
	bus := messagebus.NewMessageBus(network)

	storage := storage.NewFileStorage()

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
			ReplicaID:              uint16(i + 1),
			ReplicaAddress:         replicaAddress,
			Replicas:               configReplicas,
			LeaderElectionTimeout:  300 * time.Millisecond,
			LeaderHeartbeatTimeout: 100 * time.Millisecond,
		}

		kv := kv.NewKvStore(bus)
		raft, err := raft.NewRaft(config, bus, storage, kv, logger)
		if err != nil {
			panic(err)
		}
		replicas = append(replicas, TestReplica{Raft: raft, Kv: kv})
	}

	return Cluster{Replicas: replicas, Network: network, Bus: bus, Storage: storage}
}
