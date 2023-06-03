package simulator_test

import (
	cryptorand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/raft"
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
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

	networkConfig := NetworkConfig{
		PathClogProbability:      0.1,
		MessageReplayProbability: 0.1,
		DropMessageProbability:   0.1,
		MaxNetworkPathClogTicks:  10_000,
		MaxMessageDelayTicks:     10_000,
	}
	network := NewNetwork(networkConfig, rand)

	replicas := make([]*raft.Raft, 0, numReplicas)

	for i := 0; i < numReplicas; i++ {
		config := raft.Config{
			ReplicaID:             uint16(i),
			ReplicaAddress:        fmt.Sprintf("localhost:800%d", i),
			LeaderElectionTimeout: 10 * time.Second,
			Replicas:              make([]raft.Replica, 0),
		}
		for j := 0; j < numReplicas; j++ {
			if i == j {
				continue
			}
			config.Replicas = append(config.Replicas, raft.Replica{
				ReplicaID:      uint16(j),
				ReplicaAddress: fmt.Sprintf("localhost:800%d", j),
			})
		}
		raft := raft.NewRaft(config, messagebus.NewMessageBus(network))
		replicas = append(replicas, raft)
	}
	network.buildNetworkPaths(replicas)

	for i := 0; i < 10_010; i++ {
		network.Tick()

		for _, replica := range replicas {
			replica.Tick()
		}
	}
}
