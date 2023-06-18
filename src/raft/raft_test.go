package raft

import (
	cryptorand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"

	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	testingclock "github.com/poorlydefinedbehaviour/raft-go/src/testing/clock"
	"github.com/poorlydefinedbehaviour/raft-go/src/testing/network"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type Cluster struct {
	Config   ClusterConfig
	Replicas []TestReplica
	Network  *network.Network
	Bus      *messagebus.MessageBus
	Rand     *rand.DefaultRandom
}

type TestReplica struct {
	*Raft
	// TODO: why does raft_test know about the kv?
	// Maybe it should use something else as the state machine
	// to avoid the import cycle kv <-> raft.
	// It could be a simpler kv for testing purposes.
	// Maybe it is possible to use only testing/cluster if the
	// import cycle is removed.
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

func (cluster *Cluster) TickUntilEveryMessageIsDelivered() {
	for cluster.Network.HasPendingMessages() {
		cluster.Tick()
	}
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

type ClusterConfig struct {
	Seed        int64
	NumReplicas uint16
	Network     network.NetworkConfig
	Raft        RaftConfig
}

type RaftConfig struct {
	ReplicaCrashProbability  float64
	MaxReplicaCrashTicks     uint64
	MaxLeaderElectionTimeout time.Duration
	MinLeaderElectionTimeout time.Duration
	LeaderHeartbeatTimeout   time.Duration
}

func defaultConfig() ClusterConfig {
	bigint, err := cryptorand.Int(cryptorand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("generating seed: %w", err))
	}

	return ClusterConfig{
		Seed:        bigint.Int64(),
		NumReplicas: 3,
		Network: network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  10_000,
			MaxMessageDelayTicks:     50,
		},
		Raft: RaftConfig{
			ReplicaCrashProbability:  0.0,
			MaxReplicaCrashTicks:     0,
			MaxLeaderElectionTimeout: 300 * time.Millisecond,
			MinLeaderElectionTimeout: 100 * time.Millisecond,
			LeaderHeartbeatTimeout:   100 * time.Millisecond,
		},
	}
}

func Setup(configs ...ClusterConfig) Cluster {
	var config ClusterConfig
	if len(configs) == 0 {
		config = defaultConfig()
	} else {
		config = configs[0]
	}

	replicaAddresses := []types.ReplicaAddress{"localhost:8001", "localhost:8002", "localhost:8003"}

	rand := rand.NewRand(0)

	network := network.New(network.NetworkConfig{
		PathClogProbability:      0.0,
		MessageReplayProbability: 0.0,
		DropMessageProbability:   0.0,
		MaxNetworkPathClogTicks:  0,
		MaxMessageDelayTicks:     0,
	},
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

		dir := path.Join(os.TempDir(), uuid.NewString())
		storage, err := storage.NewFileStorage(dir)
		if err != nil {
			panic(fmt.Sprintf("instantiating storage: %s", err.Error()))
		}

		raft, err := NewRaft(config, bus, storage, kv, rand, testingclock.NewClock())
		if err != nil {
			panic(err)
		}
		replicas = append(replicas, TestReplica{Raft: raft, Kv: kv})
	}

	return Cluster{Config: config, Replicas: replicas, Network: network, Bus: bus, Rand: rand}
}

func TestNewRaft(t *testing.T) {
	t.Parallel()

	t.Run("replica recovers previous state from disk on startup", func(t *testing.T) {
		cluster := Setup()

		replica := cluster.Replicas[0]
		assert.NoError(t, replica.newTerm(withTerm(1)))

		candidate := cluster.Replicas[1]
		assert.NoError(t, replica.newTerm(withTerm(2)))

		assert.NoError(t, replica.voteFor(candidate.Config.ReplicaID, candidate.Term()))

		kv := kv.NewKvStore(cluster.Bus)

		storage, err := storage.NewFileStorage(replica.Storage.Directory())
		assert.NoError(t, err)

		replicaAfterRestart, err := NewRaft(replica.Config, replica.messageBus, storage, kv, cluster.Rand, testingclock.NewClock())
		assert.NoError(t, err)

		assert.True(t, replicaAfterRestart.VotedForCandidateInCurrentTerm(candidate.Config.ReplicaID))
	})
}

func TestApplyCommittedEntries(t *testing.T) {
	t.Parallel()

	// TODO
}

func TestVoteFor(t *testing.T) {
	t.Parallel()

	t.Run("replica must be candidate to vote for itself", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Replicas[0]

		assert.PanicsWithValue(t, "must be a candidate to vote for itself", func() {
			_ = replica.voteFor(replica.Config.ReplicaID, replica.mutableState.currentTermState.term)
		})
	})

	t.Run("replica cannot vote again after voting for another candidate", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Replicas[0]
		candidateA := cluster.Replicas[1]
		candidateB := cluster.Replicas[2]

		assert.NoError(t, replica.voteFor(candidateA.Config.ReplicaID, candidateA.mutableState.currentTermState.term))

		expectedMessage := fmt.Sprintf("votedFor=%d cannot vote again after having voted", candidateA.Config.ReplicaID)
		assert.PanicsWithValue(t, expectedMessage, func() {
			_ = replica.voteFor(candidateB.Config.ReplicaID, candidateB.mutableState.currentTermState.term)
		})
	})

	t.Run("voting twice for the same candidate has no effect", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Replicas[0]
		candidate := cluster.Replicas[1]

		assert.NoError(t, replica.voteFor(candidate.Config.ReplicaID, candidate.mutableState.currentTermState.term))

		assert.Equal(t, replica.mutableState.currentTermState.term, candidate.mutableState.currentTermState.term)
		assert.Equal(t, replica.mutableState.currentTermState.votedFor, candidate.Config.ReplicaID)

		assert.NoError(t, replica.voteFor(candidate.Config.ReplicaID, candidate.mutableState.currentTermState.term))

		assert.Equal(t, replica.mutableState.currentTermState.term, candidate.mutableState.currentTermState.term)
		assert.Equal(t, replica.mutableState.currentTermState.votedFor, candidate.Config.ReplicaID)
	})

	t.Run("vote is persisted to stable storage", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Replicas[0]
		replica := cluster.Replicas[1]

		assert.NoError(t, replica.voteFor(candidate.Config.ReplicaID, uint64(candidate.mutableState.currentTermState.term)))

		state, err := replica.Storage.GetState()
		assert.NoError(t, err)

		assert.Equal(t, candidate.Config.ReplicaID, state.VotedFor)
	})

	t.Run("vote is persisted in memory", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Replicas[0]
		replica := cluster.Replicas[1]

		assert.NoError(t, replica.voteFor(candidate.Config.ReplicaID, uint64(candidate.mutableState.currentTermState.term)))

		assert.Equal(t, candidate.Config.ReplicaID, replica.mutableState.currentTermState.votedFor)
	})

	t.Run("replica starts new term if the candidate's term is greater than its own", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Replicas[0]

		assert.NoError(t, replica.newTerm(withTerm(4)))

		candidate := cluster.Replicas[1]
		assert.NoError(t, candidate.transitionToState(Candidate))
		assert.NoError(t, candidate.newTerm(withTerm(5)))

		assert.NoError(t, replica.voteFor(candidate.Config.ReplicaID, uint64(candidate.mutableState.currentTermState.term)))

		assert.Equal(t, candidate.mutableState.currentTermState.term, replica.mutableState.currentTermState.term)
	})

	t.Run("when voting for itself, replica increments vote count", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Replicas[0]
		assert.NoError(t, candidate.transitionToState(Candidate))

		assert.Equal(t, uint16(0), candidate.votesReceived())

		assert.NoError(t, candidate.voteFor(candidate.Config.ReplicaID, uint64(candidate.mutableState.currentTermState.term)))

		assert.Equal(t, uint16(1), candidate.votesReceived())
		assert.True(t, candidate.mutableState.currentTermState.votesReceived[candidate.Config.ReplicaID])
	})
}

func TestRemoveByReplicaID(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		replicaIDS := rapid.SliceOfDistinct(rapid.Uint16(), func(x uint16) uint16 { return x }).Draw(t, "replicaIDS")

		if len(replicaIDS) == 0 {
			return
		}

		replicas := make([]Replica, 0, len(replicaIDS))

		for _, replicaID := range replicaIDS {
			replicas = append(replicas, Replica{
				ReplicaID:      replicaID,
				ReplicaAddress: fmt.Sprintf("localhost:800%d", replicaID),
			})
		}

		replicaToRemove := replicas[0]

		actual := removeByReplicaID(replicas, replicaToRemove.ReplicaID)

		assert.NotContains(t, actual, replicaToRemove)
	})
}
