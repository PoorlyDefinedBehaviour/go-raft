package testingcluster

import (
	"container/heap"
	cryptorand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/constants"
	"github.com/poorlydefinedbehaviour/raft-go/src/kv"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/raft"
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/slicesx"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	testingclock "github.com/poorlydefinedbehaviour/raft-go/src/testing/clock"
	"github.com/poorlydefinedbehaviour/raft-go/src/testing/network"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"pgregory.net/rapid"
)

type Cluster struct {
	// The current tick.
	Ticks uint64

	// The cluster config.
	Config ClusterConfig

	// Used to simulate a replica composed of a raft state machine and a key value store.
	Replicas []*TestReplica

	// Used to simulate clients that send requests.
	Clients []*TestClient

	Network *network.Network

	Rand *rand.DefaultRandom

	// User requests that have been sent but a response has not been received for yet.
	InFlightClientRequests []InFlightRequest

	// Responses received for user requests.
	ResponsesReceived []Response
}

type TestReplica struct {
	*raft.Raft
	Kv *kv.KvStore

	// Replica is offline until tick.
	crashedUntilTick uint64

	// Is the replica running right now?
	isRunning bool
}

func (replica *TestReplica) isAlive(tick uint64) bool {
	return replica.crashedUntilTick <= tick
}

type TestClient struct {
	requests PriorityQueue
}

type ClientOp = string

const (
	ClientSetRequest ClientOp = "ClientSetRequest"
	ClientGetRequest ClientOp = "ClientGetRequest"
)

type ClientRequest struct {
	SendAtTick uint64
	Key        string
	Value      []byte
	Op         ClientOp
}

type InFlightRequest struct {
	request *ClientRequest
	doneCh  chan error
}

type Response struct {
	ReceivedAtTick uint64
	Request        *ClientRequest
	Err            error
	Value          []byte
	Found          bool
}

func (cluster *Cluster) crash(replicaID types.ReplicaID) {
	crashUntilTick := cluster.replicaCrashedUntilTick()

	cluster.debug("CRASH UNTIL_TICK=%d REPLICA=%d", crashUntilTick, replicaID)

	replica, found := slicesx.Find(cluster.Replicas, func(r **TestReplica) bool {
		return (*r).Config.ReplicaID == replicaID
	})
	if !found {
		panic(fmt.Sprintf("replica %d not found: replicas=%+v", replicaID, cluster.Replicas))
	}

	(*replica).crashedUntilTick = crashUntilTick
	(*replica).isRunning = false
}

func (cluster *Cluster) restart(replicaID types.ReplicaID) {
	cluster.debug("RESTART REPLICA=%d", replicaID)

	replica, found := slicesx.Find(cluster.Replicas, func(r **TestReplica) bool {
		return (*r).Config.ReplicaID == replicaID
	})
	if !found {
		panic(fmt.Sprintf("replica %d not found: replicas=%+v", replicaID, cluster.Replicas))
	}

	storage, err := storage.NewFileStorage((*replica).Storage.Directory())
	if err != nil {
		panic(err)
	}
	kv := kv.New((*replica).Raft.Bus)
	raft, err := raft.New(raft.Config{
		ReplicaID:                (*replica).Config.ReplicaID,
		Replicas:                 (*replica).Config.Replicas,
		MaxLeaderElectionTimeout: (*replica).Config.MaxLeaderElectionTimeout,
		MinLeaderElectionTimeout: (*replica).Config.MinLeaderElectionTimeout,
		LeaderHeartbeatTimeout:   (*replica).Config.LeaderHeartbeatTimeout,
		MaxInFlightRequests:      20,
	}, (*replica).Raft.Bus, storage, kv, cluster.Rand, (*replica).Clock)
	if err != nil {
		panic(err)
	}
	(*replica).Raft = raft
	(*replica).Kv = kv
	(*replica).crashedUntilTick = 0
	(*replica).isRunning = true
}

func (cluster *Cluster) debug(template string, args ...interface{}) {
	if !constants.Debug {
		return
	}

	message := fmt.Sprintf(template, args...)

	fmt.Printf("CLUSTER: TICK=%d %s\n",
		cluster.Ticks,
		message,
	)
}

func (cluster *Cluster) replicaCrashedUntilTick() uint64 {
	return cluster.Ticks + cluster.Rand.GenBetween(0, cluster.Config.Raft.MaxReplicaCrashTicks)
}

func (cluster *Cluster) Followers() []*TestReplica {
	replicas := make([]*TestReplica, 0)

	for _, replica := range cluster.Replicas {
		if replica.State() != raft.Leader {
			replicas = append(replicas, replica)
		}
	}

	return replicas
}

func (cluster *Cluster) MustWaitForCandidate() *TestReplica {
	return cluster.mustWaitForReplicaWithStatus(raft.Candidate)
}

func (cluster *Cluster) MustWaitForLeader() *TestReplica {
	return cluster.mustWaitForReplicaWithStatus(raft.Leader)
}

func (cluster *Cluster) mustWaitForReplicaWithStatus(state raft.State) *TestReplica {
	const maxTicks = 10_000

	for i := 0; i < maxTicks; i++ {
		cluster.Tick()

		for i := 0; i < len(cluster.Replicas); i++ {
			if cluster.Replicas[i].State() == state {
				return cluster.Replicas[i]
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
	cluster.Ticks++

	cluster.Network.Tick()

	cluster.tickClientRequests()

	cluster.tickReplicas()

	cluster.tickClientResponses()
}

func (cluster *Cluster) tickClientRequests() {
	for _, client := range cluster.Clients {
		request := client.requests.Peek()
		if request == nil {
			continue
		}

		if request.SendAtTick > cluster.Ticks {
			continue
		}

		request = heap.Pop(&client.requests).(*ClientRequest)

		// TODO: send message to any replica after replicas start redirecting requests to the leader.
		leader := cluster.Leader()
		if leader == nil {
			continue
		}

		switch request.Op {
		case ClientSetRequest:
			doneCh, err := leader.Kv.Set(request.Key, request.Value)
			if err != nil {
				panic(err)
			}
			cluster.InFlightClientRequests = append(cluster.InFlightClientRequests, InFlightRequest{
				request: request,
				doneCh:  doneCh,
			})
		case ClientGetRequest:
			request.Key = maybeExistingKvKey(cluster.Rand, leader.Kv)

			value, found := leader.Kv.Get(request.Key)

			cluster.ResponsesReceived = append(cluster.ResponsesReceived, Response{
				ReceivedAtTick: cluster.Ticks,
				Request:        request,
				Err:            nil,
				Value:          value,
				Found:          found,
			})
		default:
			panic(fmt.Sprintf("unexpected client request op: %s", request.Op))
		}

	}
}

func (cluster *Cluster) tickClientResponses() {
	for _, request := range cluster.InFlightClientRequests {
		select {
		case err := <-request.doneCh:
			cluster.ResponsesReceived = append(cluster.ResponsesReceived, Response{
				Request: request.request,
				Err:     err,
			})
		default:
		}
	}
}

func (cluster *Cluster) tickReplicas() {
	for _, replica := range cluster.Replicas {
		if replica.isAlive(cluster.Ticks) {
			if !replica.isRunning {
				cluster.restart(replica.Config.ReplicaID)
			}

			replica.Tick()

			shouldCrash := cluster.Rand.GenBool(cluster.Config.Raft.ReplicaCrashProbability)
			if shouldCrash {
				cluster.crash(replica.Config.ReplicaID)
			}
		} else {
			// Replica is dead, advance its clock to avoid leaving it too far behind other replicas.
			replica.Clock.Tick()
		}
	}
}

func (cluster *Cluster) Leader() *TestReplica {
	for _, replica := range cluster.Replicas {
		if replica.Raft.State() == raft.Leader {
			return replica
		}
	}

	return nil
}

type ClusterConfig struct {
	Seed        int64
	MaxTicks    uint64
	NumReplicas uint16
	NumClients  uint64
	Network     network.NetworkConfig
	Raft        RaftConfig
}

type RaftConfig struct {
	MaxInFlightRequests      uint16
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
		MaxTicks:    math.MaxUint64,
		NumReplicas: 3,
		Network: network.NetworkConfig{
			PathClogProbability:      0.0,
			MessageReplayProbability: 0.0,
			DropMessageProbability:   0.0,
			MaxNetworkPathClogTicks:  10_000,
			MaxMessageDelayTicks:     50,
		},
		Raft: RaftConfig{
			MaxInFlightRequests:      20_000,
			ReplicaCrashProbability:  0.0,
			MaxReplicaCrashTicks:     0,
			MaxLeaderElectionTimeout: 300 * time.Millisecond,
			MinLeaderElectionTimeout: 100 * time.Millisecond,
			LeaderHeartbeatTimeout:   100 * time.Millisecond,
		},
	}
}

func Setup(t *rapid.T, configs ...ClusterConfig) Cluster {
	assert.True(len(configs) == 0 || len(configs) == 1, "zero or one configurations are allowed")

	var config ClusterConfig
	if len(configs) == 0 {
		config = defaultConfig()
	} else {
		config = configs[0]
	}

	rand := rand.NewRand(0)

	replicaAddresses := make([]types.ReplicaAddress, 0, config.NumReplicas)
	for i := 1; i <= int(config.NumReplicas); i++ {
		replicaAddresses = append(replicaAddresses, fmt.Sprintf("localhost:800%d", i))
	}

	network := network.New(config.Network, rand)

	configReplicas := make([]types.ReplicaID, 0, len(replicaAddresses))
	for i := 1; i <= int(config.NumReplicas); i++ {
		configReplicas = append(configReplicas, uint16(i))
	}

	replicas := make([]*TestReplica, 0)

	for _, replica := range configReplicas {
		bus := messagebus.NewMessageBus(network)
		kv := kv.New(bus)

		dir := path.Join(os.TempDir(), uuid.NewString())
		storage, err := storage.NewFileStorage(dir)
		if err != nil {
			panic(fmt.Sprintf("instantiating storage: %s", err.Error()))
		}

		raft, err := raft.New(raft.Config{
			ReplicaID:                replica,
			Replicas:                 configReplicas,
			MaxLeaderElectionTimeout: config.Raft.MaxLeaderElectionTimeout,
			MinLeaderElectionTimeout: config.Raft.MinLeaderElectionTimeout,
			LeaderHeartbeatTimeout:   config.Raft.LeaderHeartbeatTimeout,
			MaxInFlightRequests:      config.Raft.MaxInFlightRequests,
		}, bus, storage, kv, rand, testingclock.NewClock())
		if err != nil {
			panic(err)
		}
		replicas = append(replicas, &TestReplica{Raft: raft, Kv: kv, isRunning: true})
	}

	replicasOnMessage := make(map[types.ReplicaID]types.MessageCallback)
	for _, replica := range replicas {
		replicasOnMessage[replica.Config.ReplicaID] = replica.OnMessage
	}
	network.Setup(replicasOnMessage)

	clients := make([]*TestClient, 0, config.NumClients)

	for i := 0; i < int(config.NumClients); i++ {
		clients = append(clients, newTestClient(t, &config))
	}

	return Cluster{Config: config, Replicas: replicas, Clients: clients, Network: network, Rand: rand}
}

func newTestClient(t *rapid.T, config *ClusterConfig) *TestClient {
	numRequests := rapid.Uint16Range(0, 1000).Draw(t, "numRequests")

	requests := make(PriorityQueue, 0)

	for i := 0; i < int(numRequests); i++ {
		heap.Push(&requests, &RequestToSend{
			Request: &ClientRequest{
				SendAtTick: rapid.Uint64Range(0, config.MaxTicks).Draw(t, "AtTick"),
				Value:      rapid.SliceOf(rapid.Byte()).Draw(t, "Value"),
				Op:         rapid.SampledFrom([]ClientOp{ClientSetRequest, ClientGetRequest}).Draw(t, "Op"),
			},
		})
	}

	return &TestClient{
		requests: requests,
	}
}

func maybeExistingKvKey(rand rand.Random, kv *kv.KvStore) string {
	if len(kv.Items) == 0 || !rand.GenBool(0.5) {
		return "unkown"
	}

	index := rand.GenBetween(0, uint64(len(kv.Items)-1))

	i := 0
	for key := range kv.Items {
		if i == int(index) {
			return key

		}

		i++
	}

	panic("unreachable")
}
