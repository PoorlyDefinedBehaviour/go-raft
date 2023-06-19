package simulator_test

import (
	cryptorand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/raft"
	testingcluster "github.com/poorlydefinedbehaviour/raft-go/src/testing/cluster"
	"github.com/poorlydefinedbehaviour/raft-go/src/testing/network"
	"github.com/stretchr/testify/assert"
)

func TestSimulate(t *testing.T) {
	t.Parallel()

	bigint, err := cryptorand.Int(cryptorand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("generating seed: %w", err))
	}
	seed := bigint.Int64()

	cluster := testingcluster.Setup(testingcluster.ClusterConfig{
		Seed:        seed,
		NumReplicas: 3,
		NumClients:  3,
		Network: network.NetworkConfig{
			PathClogProbability:      0.001,
			MessageReplayProbability: 0.001,
			DropMessageProbability:   0.001,
			MaxNetworkPathClogTicks:  10_000,
			MaxMessageDelayTicks:     50,
		},
		Raft: testingcluster.RaftConfig{
			ReplicaCrashProbability:  0.001,
			MaxReplicaCrashTicks:     100,
			MaxLeaderElectionTimeout: 300 * time.Millisecond,
			MinLeaderElectionTimeout: 100 * time.Millisecond,
			LeaderHeartbeatTimeout:   100 * time.Millisecond,
		},
	})

	for i := 0; i < 500; i++ {
		cluster.Tick()

		ensureTheresZeroOrOneLeader(t, &cluster)
		ensureLogConsistency(t, &cluster)
	}
}

func ensureLogConsistency(t *testing.T, cluster *testingcluster.Cluster) {
	// TODO
}

func ensureTheresZeroOrOneLeader(t *testing.T, cluster *testingcluster.Cluster) {
	leadersPerTerm := make(map[uint64]uint64, 0)

	for _, replica := range cluster.Replicas {
		if replica.State() == raft.Leader {
			leadersPerTerm[replica.Term()]++
		}
	}

	for _, leadersInTheTerm := range leadersPerTerm {
		assert.Truef(t, leadersInTheTerm <= 1, "unexpected number of leaders: %d", leadersInTheTerm)
	}
}
