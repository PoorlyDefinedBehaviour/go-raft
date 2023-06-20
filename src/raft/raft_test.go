package raft

import (
	cryptorand "crypto/rand"
	"encoding/json"
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

		raft, err := New(config, bus, storage, kv, rand, testingclock.NewClock())
		if err != nil {
			panic(err)
		}
		replicas = append(replicas, TestReplica{Raft: raft, Kv: kv})
	}

	replicasOnMessage := make(map[types.ReplicaAddress]types.MessageCallback)
	for _, replica := range replicas {
		replicasOnMessage[replica.Config.ReplicaAddress] = replica.OnMessage
	}
	network.Setup(replicasOnMessage)

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

		replicaAfterRestart, err := New(replica.Config, replica.bus, storage, kv, cluster.Rand, testingclock.NewClock())
		assert.NoError(t, err)

		assert.True(t, replicaAfterRestart.VotedForCandidateInCurrentTerm(candidate.Config.ReplicaID))
	})
}

func TestTransitionToState(t *testing.T) {
	t.Parallel()

	t.Run("when a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log", func(t *testing.T) {
		cluster := Setup()

		replica := cluster.Replicas[0]

		lastLogIndex := replica.Storage.LastLogIndex()

		assert.NoError(t, replica.transitionToState(Leader))

		for _, otherReplica := range cluster.Replicas {
			if otherReplica == replica {
				continue
			}

			assert.Equal(t, lastLogIndex+1, replica.mutableState.nextIndex[otherReplica.Config.ReplicaID])
		}
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

func TestHandleMessageAppendEntriesInput(t *testing.T) {
	t.Parallel()

	t.Run("leader term is smaller than the replica term, success=false", func(t *testing.T) {
		t.Parallel()

		const term = 2

		cluster := Setup()

		leader := cluster.Replicas[0]
		replica := cluster.Replicas[1]

		assert.NoError(t, replica.newTerm(withTerm(term)))

		inputMessage := types.AppendEntriesInput{
			LeaderID: leader.Config.ReplicaID,
			// And the leader is at term 1.
			LeaderTerm:        1,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries:           make([]types.Entry, 0),
		}

		outgoingMessage, err := replica.handleMessage(&inputMessage)
		assert.NoError(t, err)

		expected := &types.AppendEntriesOutput{
			ReplicaID:        replica.Config.ReplicaID,
			CurrentTerm:      term,
			Success:          false,
			PreviousLogIndex: 0,
			PreviousLogTerm:  0,
		}

		assert.Equal(t, expected, outgoingMessage)
	})

	t.Run("leader message previous log index is not the same as the replicas last log index, success=false", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		leader := cluster.Replicas[0]
		replica := cluster.Replicas[1]

		appendEntriesInput := types.AppendEntriesInput{
			LeaderID:          leader.Config.ReplicaID,
			LeaderTerm:        leader.mutableState.currentTermState.term,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  1,
			PreviousLogTerm:   1,
			Entries:           make([]types.Entry, 0),
		}

		outgoingMessage, err := replica.handleMessage(&appendEntriesInput)
		assert.NoError(t, err)

		expected := &types.AppendEntriesOutput{
			ReplicaID:        replica.Config.ReplicaID,
			CurrentTerm:      leader.Term(),
			Success:          false,
			PreviousLogIndex: 0,
			PreviousLogTerm:  0,
		}

		assert.Equal(t, expected, outgoingMessage)
	})

	t.Run("replica has entry with a different term at index, should truncate replica's log", func(t *testing.T) {
		t.Parallel()

		// TODO
	})

	t.Run("appends entries to the log, success=true", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		leader := cluster.Replicas[0]
		replica := cluster.Replicas[1]

		inputMessage := types.AppendEntriesInput{
			LeaderID:          leader.Config.ReplicaID,
			LeaderTerm:        leader.Term(),
			LeaderCommitIndex: 0,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries: []types.Entry{
				{
					Term: 2,
				},
			},
		}

		outgoingMessage, err := replica.handleMessage(&inputMessage)
		assert.NoError(t, err)

		expected := &types.AppendEntriesOutput{
			ReplicaID:        replica.Config.ReplicaID,
			CurrentTerm:      leader.Term(),
			Success:          true,
			PreviousLogIndex: 0,
			PreviousLogTerm:  0,
		}

		assert.Equal(t, expected, outgoingMessage)

		entry, err := replica.Storage.GetEntryAtIndex(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), entry.Term)
	})

	t.Run("leader commit index is greater than the replica commit index, should apply entries to state machine", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		leader := cluster.Replicas[0]
		replica := cluster.Replicas[1]

		assert.NoError(t, leader.newTerm(withTerm(1)))

		entryValue, err := json.Marshal(map[string]any{
			"key":   "key1",
			"value": []byte("value1"),
		})
		assert.NoError(t, err)

		_, err = replica.handleMessage(&types.AppendEntriesInput{
			LeaderID:          leader.Config.ReplicaID,
			LeaderTerm:        leader.mutableState.currentTermState.term,
			LeaderCommitIndex: leader.mutableState.commitIndex,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries: []types.Entry{
				{
					Term:  leader.mutableState.currentTermState.term,
					Type:  2,
					Value: entryValue,
				},
			},
		})
		assert.NoError(t, err)

		assert.NoError(t, leader.newTerm(withTerm(2)))
		leader.mutableState.commitIndex = 1

		entryValue, err = json.Marshal(map[string]string{
			"key":   "key2",
			"value": "value2",
		})
		assert.NoError(t, err)

		outgoingMessage, err := replica.handleMessage(&types.AppendEntriesInput{
			LeaderID:          leader.Config.ReplicaID,
			LeaderTerm:        leader.mutableState.currentTermState.term,
			LeaderCommitIndex: leader.mutableState.commitIndex,
			PreviousLogIndex:  1,
			PreviousLogTerm:   leader.mutableState.currentTermState.term - 1,
			Entries: []types.Entry{
				{
					Term:  leader.mutableState.currentTermState.term,
					Type:  2,
					Value: entryValue,
				},
			},
		})
		assert.NoError(t, err)

		response := outgoingMessage.(*types.AppendEntriesOutput)
		assert.True(t, response.Success)

		// First entry has been applied.
		value, ok := replica.Kv.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, []byte("value1"), value)

		// Second entry has not been applied yet.
		_, ok = replica.Kv.Get("key2")
		assert.False(t, ok)
	})

	t.Run("replica updates its term if it is out of date", func(t *testing.T) {
		t.Parallel()

		// TODO
	})
}

func TestHandleMessageUserRequestInput(t *testing.T) {
	t.Parallel()

	t.Run("request completes after entries being replicated to the majority of replicas", func(t *testing.T) {
		t.Parallel()

		// TODO

		// cluster := Setup()

		// leader := cluster.MustWaitForLeader()

		// value, err := json.Marshal(map[string]any{
		// 	"key":   "key",
		// 	"value": []byte("value"),
		// })
		// assert.NoError(t, err)

		// doneCh, err := leader.HandleUserRequest(context.Background(), kv.SetCommand, value)
		// assert.NoError(t, err)
		// fmt.Printf("\n\naaaaaaa doneCh %+v\n\n", doneCh)

		// cluster.TickUntilEveryMessageIsDelivered()

		// fmt.Printf("\n\naaaaaaa before err = <-request.DoneCh\n\n")
		// err = <-doneCh
		// assert.NoError(t, err)
		// fmt.Printf("\n\naaaaaaa after err = <-request.DoneCh\n\n")

		// for i := 1; i <= int(leader.Storage.LastLogIndex()); i++ {
		// 	leaderEntry, err := leader.Storage.GetEntryAtIndex(uint64(i))
		// 	assert.NoError(t, err)

		// 	for _, replica := range cluster.Followers() {
		// 		replicaEntry, err := replica.Storage.GetEntryAtIndex(uint64(i))
		// 		assert.NoError(t, err)
		// 		assert.Equal(t, *leaderEntry, *replicaEntry)
		// 	}
		// }
	})
}

func TestHandleMessageRequestVoteInput(t *testing.T) {
	t.Parallel()

	t.Run("candidate's term is less than the replica's term, should not grant vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		assert.NoError(t, replica.newTerm(withTerm(1)))

		outgoingMessage, err := replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidate.Config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response := outgoingMessage.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate's log is smaller than the replica's log, should not grante vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		assert.NoError(t, replica.Storage.AppendEntries([]types.Entry{{Term: 0}}))

		outgoingMessage, err := replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidate.Config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response := outgoingMessage.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate last log term is smaller than the replica's last log term, should not grant vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		assert.NoError(t, replica.Storage.AppendEntries([]types.Entry{{Term: 1}}))

		outgoingMessage, err := replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidate.Config.ReplicaID,
			CandidateTerm:         1,
			CandidateLastLogIndex: 1,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response := outgoingMessage.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate log is up to date and replica has not voted yet, should grant vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		outgoingMessage, err := replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidate.Config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response := outgoingMessage.(*types.RequestVoteOutput)
		assert.True(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate log is up to date and replica has already voted for the candidate, should grant vote to same candidate again", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		for i := 0; i < 2; i++ {
			outgoingMessage, err := replica.handleMessage(&types.RequestVoteInput{
				CandidateID:           candidate.Config.ReplicaID,
				CandidateTerm:         0,
				CandidateLastLogIndex: 0,
				CandidateLastLogTerm:  0,
			})
			assert.NoError(t, err)

			response := outgoingMessage.(*types.RequestVoteOutput)
			assert.True(t, response.VoteGranted)
			assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
		}
	})

	t.Run("candidate log is up to date but replica voted for another candidate already, should not grant vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidateA := env.Replicas[0]
		candidateB := env.Replicas[1]
		replica := env.Replicas[2]

		// Request vote for candidate A.
		outgoingMessage, err := replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidateA.Config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response := outgoingMessage.(*types.RequestVoteOutput)
		assert.True(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)

		// Request vote for candidate B
		outgoingMessage, err = replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidateB.Config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response = outgoingMessage.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})
}
