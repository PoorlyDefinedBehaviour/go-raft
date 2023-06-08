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
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestSimulate(t *testing.T) {
	t.Parallel()

	log, err := zap.NewProduction(zap.WithCaller(true))
	if err != nil {
		panic(err)
	}
	logger := log.Sugar()

	const numReplicas = 3

	bigint, err := cryptorand.Int(cryptorand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("generating seed: %w", err))
	}
	seed := bigint.Int64()

	rand := rand.NewRand(seed)

	networkConfig := network.NetworkConfig{
		PathClogProbability:      0.1,
		MessageReplayProbability: 0.1,
		DropMessageProbability:   0.1,
		MaxNetworkPathClogTicks:  10_000,
		MaxMessageDelayTicks:     10_000,
	}

	replicaAddresses := make([]types.ReplicaAddress, 0, numReplicas)
	for i := 1; i <= numReplicas; i++ {
		replicaAddresses = append(replicaAddresses, fmt.Sprintf("localhost:800%d", i))
	}

	network := network.NewNetwork(networkConfig, logger, rand, replicaAddresses)

	replicas := make([]*raft.Raft, 0, numReplicas)

	for i := 1; i <= numReplicas; i++ {
		config := raft.Config{
			ReplicaID:              uint16(i),
			ReplicaAddress:         fmt.Sprintf("localhost:800%d", i),
			LeaderElectionTimeout:  300 * time.Millisecond,
			LeaderHeartbeatTimeout: 100 * time.Millisecond,
			Replicas:               make([]raft.Replica, 0),
		}
		for j := 1; j <= numReplicas; j++ {
			if i == j {
				continue
			}

			config.Replicas = append(config.Replicas, raft.Replica{
				ReplicaID:      uint16(j),
				ReplicaAddress: fmt.Sprintf("localhost:800%d", j),
			})
		}
		bus := messagebus.NewMessageBus(network)
		raft, err := raft.NewRaft(config, bus, storage.NewFileStorage(), kv.NewKvStore(bus), rand, logger)
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
