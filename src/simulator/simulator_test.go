package simulator_test

import (
	cryptorand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/raft"
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	"github.com/poorlydefinedbehaviour/raft-go/src/testing/network"
	"github.com/stretchr/testify/assert"
)

func TestSimulate(t *testing.T) {
	t.Parallel()

	const numReplicas = 3

	bigint, err := cryptorand.Int(cryptorand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("generating seed: %w", err))
	}
	seed := bigint.Uint64()

	rand := rand.NewRand(seed)

	networkConfig := network.NetworkConfig{
		PathClogProbability:      0.1,
		MessageReplayProbability: 0.1,
		DropMessageProbability:   0.1,
		MaxNetworkPathClogTicks:  10_000,
		MaxMessageDelayTicks:     10_000,
	}
	network := network.NewNetwork(networkConfig, rand, []string{})

	replicas := make([]*raft.Raft, 0, numReplicas)

	for i := 0; i < numReplicas; i++ {
		replicaiID := i + 1
		config := raft.Config{
			ReplicaID:             uint16(replicaiID),
			ReplicaAddress:        fmt.Sprintf("localhost:800%d", replicaiID),
			LeaderElectionTimeout: 10 * time.Second,
			Replicas:              make([]raft.Replica, 0),
		}
		for j := 0; j < numReplicas; j++ {
			if i == j {
				continue
			}
			otherReplicaID := j + i

			config.Replicas = append(config.Replicas, raft.Replica{
				ReplicaID:      uint16(otherReplicaID),
				ReplicaAddress: fmt.Sprintf("localhost:800%d", otherReplicaID),
			})
		}
		raft, err := raft.NewRaft(config, messagebus.NewMessageBus(network), storage.NewFileStorage(), kv.NewKvStore[string, string]())
		assert.NoError(t, err)
		replicas = append(replicas, raft)
	}

	for i := 0; i < 10_010; i++ {
		network.Tick()

		for _, replica := range replicas {
			replica.Tick()
		}
	}
}
