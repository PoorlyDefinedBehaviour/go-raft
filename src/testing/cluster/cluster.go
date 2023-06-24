package testingcluster

import (
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
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	testingclock "github.com/poorlydefinedbehaviour/raft-go/src/testing/clock"
	"github.com/poorlydefinedbehaviour/raft-go/src/testing/network"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type Cluster struct {
	Ticks    uint64
	Config   ClusterConfig
	Replicas []*TestReplica
	Clients  []*TestClient
	Network  *network.Network
	Bus      *messagebus.MessageBus
	Rand     *rand.DefaultRandom
}

type TestReplica struct {
	*raft.Raft
	Kv *kv.KvStore

	// Replica is offline until tick.
	crashedUntilTick uint64

	// Is the replica running right now?
	isRunning bool
}

func (replica *TestReplica) crash(cluster *Cluster) {
	fmt.Println("replica.crash()")
	crashUntilTick := cluster.replicaCrashedUntilTick()

	cluster.debug("CRASH UNTIL_TICK=%d REPLICA=%d", crashUntilTick, replica.Config.ReplicaID)

	replica.crashedUntilTick = crashUntilTick
	replica.isRunning = false
}

func (replica *TestReplica) restart(cluster *Cluster) {
	fmt.Println("replica.restart()")
	cluster.debug("RESTART REPLICA=%d", replica.Config.ReplicaID)

	storage, err := storage.NewFileStorage(replica.Storage.Directory())
	if err != nil {
		panic(err)
	}
	kv := kv.NewKvStore(cluster.Bus)
	raft, err := raft.New(raft.Config{
		ReplicaID:                replica.Config.ReplicaID,
		Replicas:                 replica.Config.Replicas,
		MaxLeaderElectionTimeout: replica.Config.MaxLeaderElectionTimeout,
		MinLeaderElectionTimeout: replica.Config.MinLeaderElectionTimeout,
		LeaderHeartbeatTimeout:   replica.Config.LeaderHeartbeatTimeout,
	}, cluster.Bus, storage, kv, cluster.Rand, replica.Clock)
	if err != nil {
		panic(err)
	}
	replica.Raft = raft
	replica.Kv = kv
	replica.crashedUntilTick = 0
	replica.isRunning = true
}

func (replica *TestReplica) isAlive(tick uint64) bool {
	return replica.crashedUntilTick <= tick
}

type TestClient struct{}

func (client *TestClient) Tick() {

}

func (cluster *Cluster) debug(template string, args ...interface{}) {
	message := fmt.Sprintf(template, args...)

	message = fmt.Sprintf("CLUSTER: TICK=%d %s\n",
		cluster.Ticks,
		message,
	)

	if constants.Debug {
		fmt.Println(message)
	}
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
	fmt.Println("cluster.mustWaitForReplicaWithStatus()")
	const maxTicks = 10_000

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
	fmt.Println("cluster.TickUntilEveryMessageIsDelivered()")
	for cluster.Network.HasPendingMessages() {
		cluster.Tick()
	}
}

func (cluster *Cluster) Tick() {
	fmt.Printf("TICK=%d cluster.Tick()\n", cluster.Ticks)
	cluster.Ticks++

	cluster.Bus.Tick()
	cluster.Network.Tick()

	// for _, client := range cluster.Clients {
	// 	client.Tick()
	// }

	for _, replica := range cluster.Replicas {
		if replica.isAlive(cluster.Ticks) {
			if !replica.isRunning {
				replica.restart(cluster)
			}

			replica.Tick()

			// shouldCrash := cluster.Rand.GenBool(cluster.Config.Raft.ReplicaCrashProbability)
			// if shouldCrash {
			// 		replica.crash(cluster)
			// }
		} else {
			// Replica is dead, advance its clock to avoid leaving it too far behind other replicas.
			// replica.Clock.Tick()
		}
	}
}

func (cluster *Cluster) Leader() *TestReplica {
	fmt.Println("cluster.Leader()")
	for _, replica := range cluster.Replicas {
		if replica.Raft.State() == raft.Leader {
			return replica
		}
	}

	return nil
}

type ClusterConfig struct {
	Seed        int64
	NumReplicas uint16
	NumClients  uint64
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

	bus := messagebus.NewMessageBus(network)

	configReplicas := make([]types.ReplicaID, 0, len(replicaAddresses))
	for i := 1; i <= int(config.NumReplicas); i++ {
		configReplicas = append(configReplicas, uint16(i))
	}

	replicas := make([]*TestReplica, 0)

	for _, replica := range configReplicas {
		kv := kv.NewKvStore(bus)

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
			MaxInFlightRequests:      20,
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
		clients = append(clients, &TestClient{})
	}

	return Cluster{Config: config, Replicas: replicas, Clients: clients, Network: network, Bus: bus, Rand: rand}
}
