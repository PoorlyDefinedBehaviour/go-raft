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
	"github.com/poorlydefinedbehaviour/raft-go/src/constants"
	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/slicesx"
	"github.com/stretchr/testify/assert"

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
	ticks    uint64
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

	// Replica is offline until tick.
	crashedUntilTick uint64

	// Is the replica running right now?
	isRunning bool
}

func (replica *TestReplica) TickUntilElectionTimeout() {
	timeoutAt := replica.leaderElectionTimeout.After() - replica.leaderElectionTimeout.Ticks()

	for i := uint64(0); i < timeoutAt; i++ {
		replica.Tick()
	}
}

func (replica *TestReplica) TickUntilHeartbeatTimeout() {
	timeoutAt := replica.heartbeatTimeout.After() - replica.heartbeatTimeout.Ticks()

	for i := uint64(0); i < timeoutAt; i++ {
		replica.Tick()
	}
}

func (replica *TestReplica) UntilUntilHeartbeatTImeout() {
	timeoutAt := replica.heartbeatTimeout.After() - replica.heartbeatTimeout.Ticks()

	for i := uint64(0); i < timeoutAt; i++ {
		replica.Tick()
	}
}

func (cluster *Cluster) crash(replicaID types.ReplicaID) {
	crashUntilTick := cluster.replicaCrashedUntilTick()

	cluster.debug("CRASH UNTIL_TICK=%d REPLICA=%d", crashUntilTick, replicaID)

	replica, found := slicesx.Find(cluster.Replicas, func(r *TestReplica) bool {
		return r.Config.ReplicaID == replicaID
	})
	if !found {
		panic(fmt.Sprintf("replica %d not found: replicas=%+v", replicaID, cluster.Replicas))
	}

	replica.crashedUntilTick = crashUntilTick
	replica.isRunning = false
}

func (cluster *Cluster) debug(template string, args ...interface{}) {
	if !constants.Debug {
		return
	}

	message := fmt.Sprintf(template, args...)

	fmt.Printf("CLUSTER: TICK=%d %s\n",
		cluster.ticks,
		message,
	)
}

func (cluster *Cluster) replicaCrashedUntilTick() uint64 {
	return cluster.ticks + cluster.Rand.GenBetween(0, cluster.Config.Raft.MaxReplicaCrashTicks)
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

func (cluster *Cluster) MustWaitForCandidate() *TestReplica {
	return cluster.mustWaitForReplicaWithStatus(Candidate)
}

func (cluster *Cluster) MustWaitForLeader() *TestReplica {
	return cluster.mustWaitForReplicaWithStatus(Leader)
}

func (cluster *Cluster) mustWaitForReplicaWithStatus(state State) *TestReplica {
	const maxTicks = 500

	for i := 0; i < maxTicks; i++ {
		cluster.Tick()

		for i := 0; i < len(cluster.Replicas); i++ {
			if !cluster.Replicas[i].isRunning {
				continue
			}
			if cluster.Replicas[i].State() == state {
				return &cluster.Replicas[i]
			}
		}
	}

	panic(fmt.Sprintf("unable to find replica with %s state", state))
}

func (cluster *Cluster) TickUntilEveryMessageIsDelivered() {
	for cluster.Network.HasPendingMessages() {
		cluster.Tick()
	}
}

func (cluster *Cluster) Tick() {
	cluster.ticks++
	cluster.Bus.Tick()
	cluster.Network.Tick()

	for _, replica := range cluster.Replicas {
		if replica.isRunning {
			replica.Raft.Tick()
		}
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
	MaxInFlightRequests      uint16
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
			MaxInFlightRequests:      20,
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

	configReplicas := make([]types.ReplicaID, 0, len(replicaAddresses))
	for i := 1; i <= int(config.NumReplicas); i++ {
		configReplicas = append(configReplicas, uint16(i))
	}

	replicas := make([]TestReplica, 0)

	for _, replica := range configReplicas {
		config := Config{
			ReplicaID:                replica,
			Replicas:                 configReplicas,
			MaxLeaderElectionTimeout: 300 * time.Millisecond,
			MinLeaderElectionTimeout: 100 * time.Millisecond,
			LeaderHeartbeatTimeout:   100 * time.Millisecond,
			MaxInFlightRequests:      config.Raft.MaxInFlightRequests,
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
		replicas = append(replicas, TestReplica{Raft: raft, Kv: kv, isRunning: true})
	}

	replicasOnMessage := make(map[types.ReplicaID]types.MessageCallback)
	for _, replica := range replicas {
		replicasOnMessage[replica.Config.ReplicaID] = replica.OnMessage
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

		outgoingMessages, err := replica.handleMessage(&inputMessage)
		assert.NoError(t, err)

		expected := []OutgoingMessage{
			{
				To: leader.Config.ReplicaID,
				Message: &types.AppendEntriesOutput{
					ReplicaID:        replica.Config.ReplicaID,
					CurrentTerm:      term,
					Success:          false,
					PreviousLogIndex: 0,
					PreviousLogTerm:  0,
				},
			},
		}

		assert.Equal(t, expected, outgoingMessages)
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

		outgoingMessages, err := replica.handleMessage(&appendEntriesInput)
		assert.NoError(t, err)

		expected := []OutgoingMessage{
			{
				To: leader.Config.ReplicaID,
				Message: &types.AppendEntriesOutput{
					ReplicaID:        replica.Config.ReplicaID,
					CurrentTerm:      leader.Term(),
					Success:          false,
					PreviousLogIndex: 0,
					PreviousLogTerm:  0,
				},
			},
		}

		assert.Equal(t, expected, outgoingMessages)
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

		outgoingMessages, err := replica.handleMessage(&inputMessage)
		assert.NoError(t, err)

		assert.Equal(t, 1, len(outgoingMessages))
		message := outgoingMessages[0].Message.(*types.AppendEntriesOutput)
		assert.Equal(t, leader.Config.ReplicaID, outgoingMessages[0].To)
		assert.True(t, message.Success)

		entry, err := replica.Storage.GetEntryAtIndex(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), entry.Term)
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

		outgoingMessages, err := replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidate.Config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response := outgoingMessages[0].Message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate's log is smaller than the replica's log, should not grante vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		assert.NoError(t, replica.Storage.AppendEntries([]types.Entry{{Term: 0}}))

		outgoingMessages, err := replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidate.Config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response := outgoingMessages[0].Message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate last log term is smaller than the replica's last log term, should not grant vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		assert.NoError(t, replica.Storage.AppendEntries([]types.Entry{{Term: 1}}))

		outgoingMessages, err := replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidate.Config.ReplicaID,
			CandidateTerm:         1,
			CandidateLastLogIndex: 1,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response := outgoingMessages[0].Message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate log is up to date and replica has not voted yet, should grant vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		outgoingMessages, err := replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidate.Config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response := outgoingMessages[0].Message.(*types.RequestVoteOutput)
		assert.True(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate log is up to date and replica has already voted for the candidate, should grant vote to same candidate again", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		for i := 0; i < 2; i++ {
			outgoingMessages, err := replica.handleMessage(&types.RequestVoteInput{
				CandidateID:           candidate.Config.ReplicaID,
				CandidateTerm:         0,
				CandidateLastLogIndex: 0,
				CandidateLastLogTerm:  0,
			})
			assert.NoError(t, err)

			response := outgoingMessages[0].Message.(*types.RequestVoteOutput)
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
		outgoingMessages, err := replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidateA.Config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response := outgoingMessages[0].Message.(*types.RequestVoteOutput)
		assert.True(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)

		// Request vote for candidate B
		outgoingMessages, err = replica.handleMessage(&types.RequestVoteInput{
			CandidateID:           candidateB.Config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})
		assert.NoError(t, err)

		response = outgoingMessages[0].Message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("leader crashes and a new leader is elected", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		leader := cluster.MustWaitForLeader()

		cluster.crash(leader.Config.ReplicaID)

		newLeader := cluster.MustWaitForLeader()

		assert.NotEqual(t, leader.Config.ReplicaID, newLeader.Config.ReplicaID)
	})
}

func TestHandleAppendEntriesOutput(t *testing.T) {
	t.Parallel()

	t.Run("leader sends message to replica; replica replies with a greater term; leader becomes follower", func(t *testing.T) {
		t.Parallel()

		// TODO
	})

	t.Run("leader advances next inded for replica after a successful append entries request", func(t *testing.T) {
		t.Parallel()

		// TODO
	})
}

func TestLeader(t *testing.T) {
	t.Parallel()

	t.Run("new leader commits empty entry", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		leader := cluster.MustWaitForLeader()

		entry, err := leader.Storage.GetEntryAtIndex(1)
		assert.NoError(t, err)

		assert.Equal(t, &types.Entry{
			Term:  leader.mutableState.currentTermState.term,
			Index: 1,
			Type:  types.NewLeaderEntryType,
			Value: nil,
		}, entry)
	})
}

func TestFollower(t *testing.T) {
	t.Parallel()

	t.Run("follower transitions to candidate when leader election timeout fires", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Followers()[0]

		assert.Equal(t, Follower, replica.State())

		replica.TickUntilElectionTimeout()

		assert.Equal(t, Candidate, replica.State())
	})
}

func TestCandidate(t *testing.T) {
	t.Parallel()

	t.Run("candidate transitions to follower when there's another replica with a term >= to its term", func(t *testing.T) {
		t.Parallel()

		t.Run("append entries request", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			candidate := cluster.Replicas[0]
			assert.NoError(t, candidate.transitionToState(Candidate))

			leader := cluster.Replicas[1]
			assert.NoError(t, leader.transitionToState(Leader))
			assert.NoError(t, leader.newTerm(withTerm(candidate.mutableState.currentTermState.term+1)))

			outgoingMessages, err := candidate.handleMessage(&types.AppendEntriesInput{
				LeaderID:          leader.Config.ReplicaID,
				LeaderTerm:        leader.mutableState.currentTermState.term,
				LeaderCommitIndex: 0,
				PreviousLogIndex:  0,
				PreviousLogTerm:   0,
				Entries:           make([]types.Entry, 0),
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(outgoingMessages))

			assert.True(t, outgoingMessages[0].Message.(*types.AppendEntriesOutput).Success)

			assert.Equal(t, Follower, candidate.State())
			assert.Equal(t, leader.mutableState.currentTermState.term, candidate.mutableState.currentTermState.term)
		})

		t.Run("request vote request", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			candidateA := cluster.Replicas[0]
			assert.NoError(t, candidateA.transitionToState(Candidate))

			candidateB := cluster.Replicas[1]
			assert.NoError(t, candidateB.transitionToState(Candidate))

			assert.NoError(t, candidateA.newTerm(withTerm(candidateB.mutableState.currentTermState.term+1)))

			cluster.Bus.Send(candidateA.Config.ReplicaID, candidateB.Config.ReplicaID, &types.RequestVoteInput{
				MessageID:             1,
				CandidateID:           candidateA.Config.ReplicaID,
				CandidateTerm:         candidateA.mutableState.currentTermState.term,
				CandidateLastLogIndex: 0,
				CandidateLastLogTerm:  0,
			},
			)

			cluster.Bus.Tick()
			cluster.Network.Tick()

			candidateB.Tick()

			assert.Equal(t, Follower, candidateB.State())
			assert.Equal(t, candidateA.mutableState.currentTermState.term, candidateB.mutableState.currentTermState.term)
		})
	})

	t.Run("leader election timeout is reset when follower transitions to candidate", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Followers()[0]

		replica.TickUntilElectionTimeout()

		assert.Equal(t, Candidate, replica.State())
		assert.False(t, replica.leaderElectionTimeout.Fired())
	})

	t.Run("candidate starts an election after transitioning from follower->candidate", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Followers()[0]

		candidate.TickUntilElectionTimeout()

		expected := []types.Message{
			&types.RequestVoteInput{
				MessageID:             1,
				CandidateID:           1,
				CandidateTerm:         1,
				CandidateLastLogIndex: 0,
				CandidateLastLogTerm:  0,
			},
		}

		for _, replica := range cluster.Replicas {
			if replica.Config.ReplicaID == candidate.Config.ReplicaID {
				continue
			}

			messages := cluster.Network.MessagesFromTo(candidate.Config.ReplicaID, replica.Config.ReplicaID)
			assert.EqualValues(t, expected, messages)
		}
	})

	t.Run("election timeout: candidate restarts election", func(t *testing.T) {
		cluster := Setup()

		// Replica became candidate and started election.
		candidate := cluster.MustWaitForCandidate()

		termBeforeElection := candidate.mutableState.currentTermState.term

		// Election timed out, starts a new election.
		candidate.TickUntilElectionTimeout()

		// New election, new term.
		assert.Equal(t, termBeforeElection+1, candidate.mutableState.currentTermState.term)

		expected := []types.Message{
			// Message from first election.
			&types.RequestVoteInput{
				MessageID:             1,
				CandidateID:           candidate.Config.ReplicaID,
				CandidateTerm:         1,
				CandidateLastLogIndex: 0,
				CandidateLastLogTerm:  0,
			},
			// Message from second election.
			&types.RequestVoteInput{
				MessageID:             2,
				CandidateID:           candidate.Config.ReplicaID,
				CandidateTerm:         2,
				CandidateLastLogIndex: 0,
				CandidateLastLogTerm:  0,
			},
		}

		// Should have sent request vote message to replicas.
		for _, replica := range cluster.Replicas {
			if replica.Config.ReplicaID == candidate.Config.ReplicaID {
				continue
			}

			messages := cluster.Network.MessagesFromTo(candidate.Config.ReplicaID, replica.Config.ReplicaID)
			assert.EqualValues(t, expected, messages)
		}
	})

	t.Run("candidate becomes leader iff it receives the majority of votes for the same term", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Replicas[0]
		assert.NoError(t, candidate.transitionToState(Candidate))

		_, err := candidate.startElection()
		assert.NoError(t, err)

		// Candidate's log is empty.
		assert.Equal(t, uint64(0), candidate.Storage.LastLogIndex())

		replicaA := cluster.Replicas[1]

		// Responses from previous terms are not taken into account.
		_, err = candidate.handleMessage(&types.RequestVoteOutput{
			MessageID:   1,
			ReplicaID:   replicaA.Config.ReplicaID,
			CurrentTerm: candidate.mutableState.currentTermState.term - 1,
			VoteGranted: true,
		})
		assert.NoError(t, err)

		assert.Equal(t, Candidate, candidate.State())

		// Duplicated messages are ignored.
		_, err = candidate.handleMessage(&types.RequestVoteOutput{
			MessageID:   1,
			ReplicaID:   replicaA.Config.ReplicaID,
			CurrentTerm: candidate.mutableState.currentTermState.term,
			VoteGranted: true,
		})
		assert.NoError(t, err)

		// Got majority votes, becomes leader.
		assert.Equal(t, Leader, candidate.State())
	})
}
