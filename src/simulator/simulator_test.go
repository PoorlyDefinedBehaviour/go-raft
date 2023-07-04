package simulator_test

import (
	cryptorand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/raft"
	"github.com/poorlydefinedbehaviour/raft-go/src/slicesx"
	testingcluster "github.com/poorlydefinedbehaviour/raft-go/src/testing/cluster"
	"github.com/poorlydefinedbehaviour/raft-go/src/testing/network"
	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestSimulate(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		bigint, err := cryptorand.Int(cryptorand.Reader, big.NewInt(math.MaxInt64))
		if err != nil {
			panic(fmt.Errorf("generating seed: %w", err))
		}
		seed := bigint.Int64()

		clusterConfig := testingcluster.ClusterConfig{
			Seed:        seed,
			MaxTicks:    50_000,
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
				MaxInFlightRequests:      20_000,
				ReplicaCrashProbability:  0.0001,
				MaxReplicaCrashTicks:     100,
				MaxLeaderElectionTimeout: 300 * time.Millisecond,
				MinLeaderElectionTimeout: 100 * time.Millisecond,
				LeaderHeartbeatTimeout:   100 * time.Millisecond,
			},
		}
		cluster := testingcluster.Setup(t, clusterConfig)

		livelockChecker := newLivelockChecker(10_000)

		for i := 0; i < int(clusterConfig.MaxTicks); i++ {
			if i%10_000 == 0 {
				fmt.Printf("Simulation tick %d\n", i)
			}

			cluster.Tick()

			ensureLinearizability(t, &cluster)
			ensureTheresZeroOrOneLeader(t, &cluster)
			ensureLogConsistency(t, &cluster)
			livelockChecker.Check(t, &cluster)

			if t.Failed() {
				fmt.Printf("[FAILED] seed: %d\n", seed)
				break
			}
		}
	})
}

func ensureLinearizability(t *rapid.T, cluster *testingcluster.Cluster) {
	for _, response := range cluster.ResponsesReceived {
		switch response.Request.Op {
		case testingcluster.ClientGetRequest:
			if response.Err != nil {
				break
			}

			// TODO: this is wrong because a response may not be sent to a successfully processed request.
			// Check if replica received and processed the request instead?
			responseToSetRequest, setRequestFound := slicesx.FindLast(cluster.ResponsesReceived, func(r *testingcluster.Response) bool {
				return r.Request.Op == testingcluster.ClientSetRequest &&
					r.Request.Key == response.Request.Key &&
					r.ReceivedAtTick >= response.Request.SendAtTick
			})

			// If a get request found a value for a key,
			// ensure that the last request for that key has the same value.
			if response.Found {
				assert.True(t, setRequestFound)
				assert.Equal(t, responseToSetRequest.Request.Value, response.Value)
			} else {
				// Get request did not find a value for a key,
				// ensure no there isn't a request that set the key before.
				assert.False(t, setRequestFound)
			}

		// After a replica returns OK for a set request, the kv must have the entry applied to it.
		case testingcluster.ClientSetRequest:
			if response.Err != nil {
				break
			}

			value, found := cluster.Leader().Kv.Get(response.Request.Key)

			assert.True(t, found)
			assert.EqualValues(t, response.Request.Value, value)

		default:
			panic(fmt.Sprintf("unexpected client request op: %s", response.Request.Op))
		}
	}
}

func ensureLogConsistency(t *rapid.T, cluster *testingcluster.Cluster) {
	return
	leader := cluster.Leader()
	if leader == nil {
		return
	}

	// TODO: compare logs(committed only?)
	for _, follower := range cluster.Followers() {
		assert.True(t, leader.Storage.LastLogIndex() >= follower.Storage.LastLogIndex())
		assert.True(t, leader.Storage.LastLogTerm() >= follower.Storage.LastLogTerm())

		fmt.Printf("REPLICA=%d follower logs: %+v\n", follower.Config.ReplicaID, follower.Storage.Debug())
	}
}

func ensureTheresZeroOrOneLeader(t *rapid.T, cluster *testingcluster.Cluster) {
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

func (checker *livelockChecker) Check(t *rapid.T, cluster *testingcluster.Cluster) {
	return
	leader := cluster.Leader()

	if leader != nil {
		checker.lastLeaderFoundAtTick = cluster.Ticks
	} else {
		assert.True(t, cluster.Ticks-checker.lastLeaderFoundAtTick < checker.maxTicksWithoutLeader)
	}
}
