package raft

import (
	"testing"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	"github.com/poorlydefinedbehaviour/raft-go/src/testing/network"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
)

type SetupOutput struct {
	replicas []*Raft
	network  *network.Network
	bus      *messagebus.MessageBus
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

	replicas := make([]*Raft, 0)

	for i, replicaAddress := range replicaAddresses {
		configReplicas := make([]Replica, 0)

		for j, otherReplicaAddress := range replicaAddresses {
			if replicaAddress == otherReplicaAddress {
				continue
			}
			configReplicas = append(configReplicas, Replica{ReplicaID: uint16(j + 1), ReplicaAddress: replicaAddress})
		}

		config := Config{
			ReplicaID:             uint16(i + 1),
			ReplicaAddress:        replicaAddress,
			Replicas:              configReplicas,
			LeaderElectionTimeout: 10 * time.Second,
		}
		raft, err := NewRaft(config, bus, storage, kv.NewKvStore())
		if err != nil {
			panic(err)
		}
		replicas = append(replicas, raft)
	}

	return SetupOutput{replicas: replicas, network: network, bus: bus}

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
			config:             Config{LeaderElectionTimeout: 5 * time.Second},
			initialCurrentTick: 0,
			expected:           false,
		},
		{
			description:        "leader election timeout is set to a tick smaller than the current tick, should return true",
			config:             Config{LeaderElectionTimeout: 5 * time.Second},
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

func TestHandleMessagesAppendEntriesRequest(t *testing.T) {
	t.Parallel()

	t.Run("leader term is smaller than the replica term, success=false", func(t *testing.T) {
		t.Parallel()

		replicas := []Replica{
			{
				ReplicaID:      1,
				ReplicaAddress: "localhost:8001",
			},
			{
				ReplicaID:      2,
				ReplicaAddress: "localhost:8002",
			},
		}

		// Replica with id 1 is the leader.
		leader := replicas[0]

		network := network.NewNetwork(network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  0,
			MaxMessageDelayTicks:     0,
		},
			rand.NewRand(0),
			[]types.ReplicaAddress{"localhost:8000", "localhost:8001", "localhost:8002"},
		)
		bus := messagebus.NewMessageBus(network)

		config := Config{
			ReplicaID:             0,
			ReplicaAddress:        "localhost:8000",
			Replicas:              replicas,
			LeaderElectionTimeout: 10 * time.Second,
		}
		raft, err := NewRaft(config, bus, storage.NewFileStorage(), kv.NewKvStore())
		assert.NoError(t, err)

		// This replica is at term 2.
		assert.NoError(t, raft.newTerm(withTerm(2)))

		bus.SendAppendEntriesRequest(leader.ReplicaAddress, config.ReplicaAddress, types.AppendEntriesInput{
			LeaderID: leader.ReplicaID,
			// And the leader is at term 1.
			LeaderTerm:        1,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries:           make([]types.Entry, 0),
		})

		network.Tick()

		raft.handleMessages()

		network.Tick()

		// The leader should receive a unsuccessful response because its term is lower than the replica's term.
		message, err := bus.Receive(leader.ReplicaAddress)
		assert.NoError(t, err)

		response := message.(*types.AppendEntriesOutput)
		assert.Equal(t, raft.mutableState.currentTermState.term, response.CurrentTerm)
		assert.False(t, response.Success)
		assert.Equal(t, uint64(0), response.PreviousLogIndex)
		assert.Equal(t, uint64(0), response.PreviousLogTerm)
	})

	t.Run("replica entry at previousLogIndex does not match previousLogTerm, success=false", func(t *testing.T) {
		t.Parallel()

		replicas := []Replica{
			{
				ReplicaID:      1,
				ReplicaAddress: "localhost:8001",
			},
			{
				ReplicaID:      2,
				ReplicaAddress: "localhost:8002",
			},
		}

		// Replica with id 1 is the leader.
		leader := replicas[0]

		network := network.NewNetwork(network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  0,
			MaxMessageDelayTicks:     0,
		},
			rand.NewRand(0),
			[]types.ReplicaAddress{"localhost:8000", "localhost:8001", "localhost:8002"},
		)
		bus := messagebus.NewMessageBus(network)

		storage := storage.NewFileStorage()
		// The last log entry is at term 2.
		assert.NoError(t, storage.AppendEntries([]types.Entry{{Term: 0}}))

		config := Config{
			ReplicaID:             0,
			ReplicaAddress:        "localhost:8000",
			Replicas:              replicas,
			LeaderElectionTimeout: 10 * time.Second,
		}
		raft, err := NewRaft(config, bus, storage, kv.NewKvStore())
		assert.NoError(t, err)

		assert.NoError(t, raft.newTerm(withTerm(0)))

		storage.AppendEntries([]types.Entry{{Term: 2}})

		bus.SendAppendEntriesRequest(leader.ReplicaAddress, config.ReplicaAddress, types.AppendEntriesInput{
			LeaderID:          leader.ReplicaID,
			LeaderTerm:        3,
			LeaderCommitIndex: 1,
			PreviousLogIndex:  1,
			PreviousLogTerm:   1,
			Entries:           make([]types.Entry, 0),
		})

		network.Tick()

		raft.handleMessages()

		network.Tick()

		// The leader should receive a unsuccessful response because its term is lower than the replica's term.
		message, err := bus.Receive(leader.ReplicaAddress)
		assert.NoError(t, err)

		response := message.(*types.AppendEntriesOutput)
		assert.Equal(t, raft.mutableState.currentTermState.term, response.CurrentTerm)
		assert.False(t, response.Success)
		assert.Equal(t, uint64(0), response.PreviousLogIndex)
		assert.Equal(t, uint64(0), response.PreviousLogTerm)
	})

	t.Run("replica entry conflicts with leader entry in the same index, should truncate the replica log", func(t *testing.T) {
		t.Parallel()

		replicas := []Replica{
			{
				ReplicaID:      1,
				ReplicaAddress: "localhost:8001",
			},
			{
				ReplicaID:      2,
				ReplicaAddress: "localhost:8002",
			},
		}

		// Replica with id 1 is the leader.
		leader := replicas[0]

		network := network.NewNetwork(network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  0,
			MaxMessageDelayTicks:     0,
		},
			rand.NewRand(0),
			[]types.ReplicaAddress{"localhost:8000", "localhost:8001", "localhost:8002"},
		)
		bus := messagebus.NewMessageBus(network)

		storage := storage.NewFileStorage()
		// The last log entry is at term 2 in the replica.
		assert.NoError(t, storage.AppendEntries([]types.Entry{
			{
				Term: 0,
			},
			{
				Term: 0,
			},
		}))

		config := Config{
			ReplicaID:             0,
			ReplicaAddress:        "localhost:8000",
			Replicas:              replicas,
			LeaderElectionTimeout: 10 * time.Second,
		}
		raft, err := NewRaft(config, bus, storage, kv.NewKvStore())
		assert.NoError(t, err)

		bus.SendAppendEntriesRequest(leader.ReplicaAddress, config.ReplicaAddress, types.AppendEntriesInput{
			LeaderID:          leader.ReplicaID,
			LeaderTerm:        2,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  1,
			PreviousLogTerm:   1,
			Entries: []types.Entry{
				{
					Term: 2,
				},
			},
		})

		network.Tick()

		raft.handleMessages()

		entry, err := raft.storage.GetEntryAtIndex(0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), entry.Term)

		entry, err = raft.storage.GetEntryAtIndex(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), entry.Term)

		_, err = raft.storage.GetEntryAtIndex(2)
		assert.Contains(t, err.Error(), "index out of bounds")
	})

	t.Run("appends entries to the log, success=true", func(t *testing.T) {
		t.Parallel()

		replicas := []Replica{
			{
				ReplicaID:      1,
				ReplicaAddress: "localhost:8001",
			},
			{
				ReplicaID:      2,
				ReplicaAddress: "localhost:8002",
			},
		}

		// Replica with id 1 is the leader.
		leader := replicas[0]

		network := network.NewNetwork(network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  0,
			MaxMessageDelayTicks:     0,
		},
			rand.NewRand(0),
			[]types.ReplicaAddress{"localhost:8000", "localhost:8001", "localhost:8002"},
		)
		bus := messagebus.NewMessageBus(network)

		storage := storage.NewFileStorage()

		config := Config{
			ReplicaID:             0,
			ReplicaAddress:        "localhost:8000",
			Replicas:              replicas,
			LeaderElectionTimeout: 10 * time.Second,
		}
		raft, err := NewRaft(config, bus, storage, kv.NewKvStore())
		assert.NoError(t, err)

		bus.SendAppendEntriesRequest(leader.ReplicaAddress, config.ReplicaAddress, types.AppendEntriesInput{
			LeaderID:          leader.ReplicaID,
			LeaderTerm:        0,
			LeaderCommitIndex: 0,
			PreviousLogIndex:  0,
			PreviousLogTerm:   0,
			Entries: []types.Entry{
				{
					Term: 0,
				},
			},
		})

		network.Tick()

		raft.handleMessages()

		network.Tick()

		message, err := bus.Receive(leader.ReplicaAddress)
		assert.NoError(t, err)

		response := message.(*types.AppendEntriesOutput)
		assert.Equal(t, raft.mutableState.currentTermState.term, response.CurrentTerm)
		assert.True(t, response.Success)
		assert.Equal(t, uint64(0), response.PreviousLogIndex)
		assert.Equal(t, uint64(0), response.PreviousLogTerm)

		entry, err := raft.storage.GetEntryAtIndex(0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), entry.Term)
	})
}

func TestHandleMessagesRequestVote(t *testing.T) {
	t.Parallel()

	t.Run("candidate's term is less than the replica's term, should not grant vote", func(t *testing.T) {
		t.Parallel()

		candidate := Replica{
			ReplicaID:      1,
			ReplicaAddress: "localhost:8001",
		}

		network := network.NewNetwork(network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  0,
			MaxMessageDelayTicks:     0,
		},
			rand.NewRand(0),
			[]types.ReplicaAddress{"localhost:8000", "localhost:8001", "localhost:8002"},
		)
		bus := messagebus.NewMessageBus(network)

		storage := storage.NewFileStorage()

		config := Config{
			ReplicaID:             0,
			ReplicaAddress:        "localhost:8000",
			Replicas:              []Replica{candidate},
			LeaderElectionTimeout: 10 * time.Second,
		}
		raft, err := NewRaft(config, bus, storage, kv.NewKvStore())
		assert.NoError(t, err)

		assert.NoError(t, raft.newTerm(withTerm(1)))

		bus.RequestVote(candidate.ReplicaAddress, config.ReplicaAddress, types.RequestVoteInput{
			CandidateID:           candidate.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})

		network.Tick()

		raft.handleMessages()

		network.Tick()

		message, err := bus.Receive(candidate.ReplicaAddress)
		assert.NoError(t, err)

		response := message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, raft.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate's log is smaller than the replica's log, should not grante vote", func(t *testing.T) {
		t.Parallel()
		candidate := Replica{
			ReplicaID:      1,
			ReplicaAddress: "localhost:8001",
		}

		network := network.NewNetwork(network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  0,
			MaxMessageDelayTicks:     0,
		},
			rand.NewRand(0),
			[]types.ReplicaAddress{"localhost:8000", "localhost:8001", "localhost:8002"},
		)
		bus := messagebus.NewMessageBus(network)

		storage := storage.NewFileStorage()

		config := Config{
			ReplicaID:             0,
			ReplicaAddress:        "localhost:8000",
			Replicas:              []Replica{candidate},
			LeaderElectionTimeout: 10 * time.Second,
		}
		raft, err := NewRaft(config, bus, storage, kv.NewKvStore())
		assert.NoError(t, err)

		raft.storage.AppendEntries([]types.Entry{{Term: 0}})

		bus.RequestVote(candidate.ReplicaAddress, config.ReplicaAddress, types.RequestVoteInput{
			CandidateID:           candidate.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})

		network.Tick()

		raft.handleMessages()

		network.Tick()

		message, err := bus.Receive(candidate.ReplicaAddress)
		assert.NoError(t, err)

		response := message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, raft.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate last log term is smaller than the replica's last log term, should not grant vote", func(t *testing.T) {
		t.Parallel()
		candidate := Replica{
			ReplicaID:      1,
			ReplicaAddress: "localhost:8001",
		}

		network := network.NewNetwork(network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  0,
			MaxMessageDelayTicks:     0,
		},
			rand.NewRand(0),
			[]types.ReplicaAddress{"localhost:8000", "localhost:8001", "localhost:8002"},
		)
		bus := messagebus.NewMessageBus(network)

		storage := storage.NewFileStorage()

		config := Config{
			ReplicaID:             0,
			ReplicaAddress:        "localhost:8000",
			Replicas:              []Replica{candidate},
			LeaderElectionTimeout: 10 * time.Second,
		}
		raft, err := NewRaft(config, bus, storage, kv.NewKvStore())
		assert.NoError(t, err)

		raft.storage.AppendEntries([]types.Entry{{Term: 1}})

		bus.RequestVote(candidate.ReplicaAddress, config.ReplicaAddress, types.RequestVoteInput{
			CandidateID:           candidate.ReplicaID,
			CandidateTerm:         1,
			CandidateLastLogIndex: 1,
			CandidateLastLogTerm:  0,
		})

		network.Tick()

		raft.handleMessages()

		network.Tick()

		message, err := bus.Receive(candidate.ReplicaAddress)
		assert.NoError(t, err)

		response := message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, raft.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate log is up to date and replica has not voted yet, should grant vote", func(t *testing.T) {
		t.Parallel()
		candidate := Replica{
			ReplicaID:      1,
			ReplicaAddress: "localhost:8001",
		}

		network := network.NewNetwork(network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  0,
			MaxMessageDelayTicks:     0,
		},
			rand.NewRand(0),
			[]types.ReplicaAddress{"localhost:8000", "localhost:8001", "localhost:8002"},
		)
		bus := messagebus.NewMessageBus(network)

		storage := storage.NewFileStorage()

		config := Config{
			ReplicaID:             0,
			ReplicaAddress:        "localhost:8000",
			Replicas:              []Replica{candidate},
			LeaderElectionTimeout: 10 * time.Second,
		}
		raft, err := NewRaft(config, bus, storage, kv.NewKvStore())
		assert.NoError(t, err)

		bus.RequestVote(candidate.ReplicaAddress, config.ReplicaAddress, types.RequestVoteInput{
			CandidateID:           candidate.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})

		network.Tick()

		raft.handleMessages()

		network.Tick()

		message, err := bus.Receive(candidate.ReplicaAddress)
		assert.NoError(t, err)

		response := message.(*types.RequestVoteOutput)
		assert.True(t, response.VoteGranted)
		assert.Equal(t, raft.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate log is up to date and replica has already voted for the candidate, should grant vote", func(t *testing.T) {
		t.Parallel()
		candidate := Replica{
			ReplicaID:      1,
			ReplicaAddress: "localhost:8001",
		}

		network := network.NewNetwork(network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  0,
			MaxMessageDelayTicks:     0,
		},
			rand.NewRand(0),
			[]types.ReplicaAddress{"localhost:8000", "localhost:8001", "localhost:8002"},
		)
		bus := messagebus.NewMessageBus(network)

		storage := storage.NewFileStorage()

		config := Config{
			ReplicaID:             0,
			ReplicaAddress:        "localhost:8000",
			Replicas:              []Replica{candidate},
			LeaderElectionTimeout: 10 * time.Second,
		}
		raft, err := NewRaft(config, bus, storage, kv.NewKvStore())
		assert.NoError(t, err)

		for i := 0; i < 2; i++ {
			bus.RequestVote(candidate.ReplicaAddress, config.ReplicaAddress, types.RequestVoteInput{
				CandidateID:           candidate.ReplicaID,
				CandidateTerm:         0,
				CandidateLastLogIndex: 0,
				CandidateLastLogTerm:  0,
			})

			network.Tick()

			raft.handleMessages()

			network.Tick()

			message, err := bus.Receive(candidate.ReplicaAddress)
			assert.NoError(t, err)

			response := message.(*types.RequestVoteOutput)
			assert.True(t, response.VoteGranted)
			assert.Equal(t, raft.mutableState.currentTermState.term, response.CurrentTerm)
		}
	})

	t.Run("candidate log is up to date but replica voted for another candidate already, should not grant vote", func(t *testing.T) {
		t.Parallel()

		env := setup()

		candidateA := env.replicas[0]
		candidateB := env.replicas[1]
		replica := env.replicas[2]

		// Request vote for candidate A.
		env.bus.RequestVote(candidateA.ReplicaAddress(), replica.ReplicaAddress(), types.RequestVoteInput{
			CandidateID:           candidateA.config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})

		env.network.Tick()

		replica.handleMessages()

		env.network.Tick()

		message, err := env.bus.Receive(candidateA.ReplicaAddress())
		assert.NoError(t, err)

		response := message.(*types.RequestVoteOutput)
		assert.True(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)

		// Request vote for candidate B
		env.bus.RequestVote(candidateB.ReplicaAddress(), replica.ReplicaAddress(), types.RequestVoteInput{
			CandidateID:           candidateB.config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})

		env.network.Tick()

		replica.handleMessages()

		env.network.Tick()

		message, err = env.bus.Receive(candidateB.ReplicaAddress())
		assert.NoError(t, err)

		response = message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})
}
