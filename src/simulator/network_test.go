package simulator_test

import (
	"github.com/poorlydefinedbehaviour/raft-go/src/raft"
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type NetworkConfig struct {
	PathClogProbability      float64
	MessageReplayProbability float64
	DropMessageProbability   float64
	MaxNetworkPathClogTicks  uint64
	MaxMessageDelayTicks     uint64
}

type Network struct {
	config NetworkConfig

	rand rand.Random

	ticks uint64

	replicas []*raft.Raft

	// The network path from replica A to replica B.
	networkPaths []NetworkPath

	// Messages that need to be sent to a replica.
	sendMessageQueue PriorityQueue
}

func (network *Network) AddReplica(raft *raft.Raft) {
	network.replicas = append(network.replicas, raft)
	buildNetworkPaths(network.replicas)
}

type NetworkPath struct {
	FromReplicaID          types.ReplicaID
	ToReplicaID            types.ReplicaID
	MakeReachableAfterTick uint64
}

func newNetworkPath(fromReplicaID, toReplicaID types.ReplicaID) NetworkPath {
	return NetworkPath{
		FromReplicaID:          fromReplicaID,
		ToReplicaID:            toReplicaID,
		MakeReachableAfterTick: 0,
	}
}

type MessageToSend struct {
	AfterTick     uint64
	FromReplicaID types.ReplicaID
	ToReplicaID   types.ReplicaID
	Message       []byte
	Index         int
}

func NewNetwork(config NetworkConfig, rand rand.Random) *Network {
	return &Network{
		config:       config,
		replicas:     make([]*raft.Raft, 0),
		rand:         rand,
		networkPaths: make([]NetworkPath, 0),
	}
}

func buildNetworkPaths(replicas []*raft.Raft) []NetworkPath {
	paths := make([]NetworkPath, 0)

	for _, fromReplica := range replicas {
		for _, toReplica := range replicas {
			if fromReplica == toReplica {
				continue
			}

			paths = append(paths, newNetworkPath(fromReplica.ReplicaID(), toReplica.ReplicaID()))
		}
	}

	return paths
}

func (network *Network) send(fromReplicaID types.ReplicaID, message []byte) {
	messageToSend := &MessageToSend{
		AfterTick:     network.randomDelay(),
		FromReplicaID: fromReplicaID,
		Message:       message,
	}

	network.sendMessageQueue.Push(messageToSend)
}

func (network *Network) randomDelay() uint64 {
	return network.ticks + network.rand.GenBetween(0, network.config.MaxMessageDelayTicks)
}

func (network *Network) Tick() {
	network.ticks++

	for i := range network.networkPaths {
		shouldMakeUnreachable := network.rand.GenBool(network.config.PathClogProbability)
		if shouldMakeUnreachable {
			network.networkPaths[i].MakeReachableAfterTick = network.rand.GenBetween(0, network.config.MaxNetworkPathClogTicks)
		}
	}

	for len(network.sendMessageQueue) > 0 {
		oldestMessage := network.sendMessageQueue.Pop().(*MessageToSend)
		if oldestMessage.AfterTick > network.ticks {
			network.sendMessageQueue.Push(oldestMessage)
			return
		}

		networkPath := network.findPath(oldestMessage.FromReplicaID, oldestMessage.ToReplicaID)
		if networkPath.MakeReachableAfterTick > network.ticks {
			network.sendMessageQueue.Push(oldestMessage)
			return
		}

		shouldDrop := network.rand.GenBool(network.config.DropMessageProbability)
		if shouldDrop {
			continue
		}

		if oldestMessage.AfterTick < network.ticks {
			panic("todo")
			// network.replicas[oldestMessage.Message.ReplicaID].onMessageReceived(oldestMessage.FromReplicaID, oldestMessage.Message)
		}

		shouldReplay := network.rand.GenBool(network.config.MessageReplayProbability)
		if shouldReplay {
			network.sendMessageQueue.Push(oldestMessage)
		}
	}
}

func (network *Network) findPath(fromReplicaID, toReplicaID types.ReplicaID) NetworkPath {
	for _, path := range network.networkPaths {
		if path.FromReplicaID == fromReplicaID && path.ToReplicaID == toReplicaID {
			return path
		}
	}
	panic("unreachable")
}
