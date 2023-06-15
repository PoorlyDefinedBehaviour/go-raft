package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/stretchr/testify/assert"
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

	t.Run("replica must be candidate to vote for itself", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Replicas[0]

		assert.PanicsWithValue(t, "must be a candidate to vote for itself", func() {
			_ = replica.voteFor(replica.config.ReplicaID, replica.mutableState.currentTermState.term)
		})
	})

	t.Run("replica cannot vote again after voting for another candidate", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Replicas[0]
		candidateA := cluster.Replicas[1]
		candidateB := cluster.Replicas[2]

		assert.NoError(t, replica.voteFor(candidateA.config.ReplicaID, candidateA.mutableState.currentTermState.term))

		expectedMessage := fmt.Sprintf("votedFor=%d cannot vote again after having voted", candidateA.config.ReplicaID)
		assert.PanicsWithValue(t, expectedMessage, func() {
			_ = replica.voteFor(candidateB.config.ReplicaID, candidateB.mutableState.currentTermState.term)
		})
	})

	t.Run("voting twice for the same candidate has no effect", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Replicas[0]
		candidate := cluster.Replicas[1]

		assert.NoError(t, replica.voteFor(candidate.config.ReplicaID, candidate.mutableState.currentTermState.term))

		assert.Equal(t, replica.mutableState.currentTermState.term, candidate.mutableState.currentTermState.term)
		assert.Equal(t, replica.mutableState.currentTermState.votedFor, candidate.config.ReplicaID)

		assert.NoError(t, replica.voteFor(candidate.config.ReplicaID, candidate.mutableState.currentTermState.term))

		assert.Equal(t, replica.mutableState.currentTermState.term, candidate.mutableState.currentTermState.term)
		assert.Equal(t, replica.mutableState.currentTermState.votedFor, candidate.config.ReplicaID)
	})

	t.Run("vote is persisted to stable storage", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Replicas[0]
		replica := cluster.Replicas[1]

		assert.NoError(t, replica.voteFor(candidate.config.ReplicaID, uint64(candidate.mutableState.currentTermState.term)))

		state, err := replica.storage.GetState()
		assert.NoError(t, err)

		assert.Equal(t, candidate.config.ReplicaID, state.VotedFor)
	})

	t.Run("vote is persisted in memory", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Replicas[0]
		replica := cluster.Replicas[1]

		assert.NoError(t, replica.voteFor(candidate.config.ReplicaID, uint64(candidate.mutableState.currentTermState.term)))

		assert.Equal(t, candidate.config.ReplicaID, replica.mutableState.currentTermState.votedFor)
	})

	t.Run("replica starts new term if the candidate's term is greater than its own", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Replicas[0]

		assert.NoError(t, replica.newTerm(withTerm(4)))

		candidate := cluster.Replicas[1]
		assert.NoError(t, candidate.transitionToState(Candidate))
		assert.NoError(t, candidate.newTerm(withTerm(5)))

		assert.NoError(t, replica.voteFor(candidate.config.ReplicaID, uint64(candidate.mutableState.currentTermState.term)))

		assert.Equal(t, candidate.mutableState.currentTermState.term, replica.mutableState.currentTermState.term)
	})

	t.Run("when voting for itself, replica increments vote count", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Replicas[0]
		assert.NoError(t, candidate.transitionToState(Candidate))

		assert.Equal(t, uint16(0), candidate.votesReceived())

		assert.NoError(t, candidate.voteFor(candidate.config.ReplicaID, uint64(candidate.mutableState.currentTermState.term)))

		assert.Equal(t, uint16(1), candidate.votesReceived())
		assert.True(t, candidate.mutableState.currentTermState.votesReceived[candidate.config.ReplicaID])
	})
}
