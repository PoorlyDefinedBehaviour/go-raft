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

	// TODO: use rapid to generate client requests

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
			MaxNetworkPathClogTicks:  1000,
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

	livelockChecker := newLivelockChecker(10_000)

	for i := 0; i < 50_000; i++ {
		cluster.Tick()

		ensureTheresZeroOrOneLeader(t, &cluster)
		ensureLogConsistency(t, &cluster)
		livelockChecker.Check(t, &cluster)
	}

	if t.Failed() {
		fmt.Printf("seed: %d\n", seed)
	}
}

func ensureLogConsistency(t *testing.T, cluster *testingcluster.Cluster) {
	leader := cluster.Leader()
	if leader == nil {
		return
	}

	fmt.Printf("REPLICA=%d leader logs: %+v\n", leader.Config.ReplicaID, leader.Storage.Debug())

	for _, follower := range cluster.Followers() {
		assert.True(t, leader.Storage.LastLogIndex() >= follower.Storage.LastLogIndex())
		assert.True(t, leader.Storage.LastLogTerm() >= follower.Storage.LastLogTerm())

		fmt.Printf("REPLICA=%d follower logs: %+v\n", follower.Config.ReplicaID, follower.Storage.Debug())
	}
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

type livelockChecker struct {
	maxTicksWithoutLeader uint64
	lastLeaderFoundAtTick uint64
}

func newLivelockChecker(maxTicksWithoutLeader uint64) *livelockChecker {
	return &livelockChecker{
		maxTicksWithoutLeader: maxTicksWithoutLeader,
		lastLeaderFoundAtTick: 0,
	}
}

func (checker *livelockChecker) Check(t *testing.T, cluster *testingcluster.Cluster) {
	return
	leader := cluster.Leader()

	if leader != nil {
		checker.lastLeaderFoundAtTick = cluster.Ticks
	} else {
		assert.True(t, cluster.Ticks-checker.lastLeaderFoundAtTick < checker.maxTicksWithoutLeader)
	}
}
