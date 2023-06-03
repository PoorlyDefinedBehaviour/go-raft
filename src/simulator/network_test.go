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

	// List of messages that are ready to be delivered for specific replicas.
	// Messages are delivered then Receive() is called.
	toDeliverMessageQueue map[types.ReplicaAddress][]types.Message
}

type NetworkPath struct {
	fromReplicaAddress     types.ReplicaAddress
	toReplicaAddress       types.ReplicaAddress
	makeReachableAfterTick uint64
}

func newNetworkPath(fromReplicaAddress, toReplicaAddress types.ReplicaAddress) NetworkPath {
	return NetworkPath{
		fromReplicaAddress:     fromReplicaAddress,
		toReplicaAddress:       toReplicaAddress,
		makeReachableAfterTick: 0,
	}
}

type MessageToSend struct {
	AfterTick          uint64
	FromReplicaAddress types.ReplicaAddress
	ToReplicaAddress   types.ReplicaAddress
	Message            types.Message
	Index              int
}

func NewNetwork(config NetworkConfig, rand rand.Random) *Network {
	return &Network{
		config:                config,
		replicas:              make([]*raft.Raft, 0),
		rand:                  rand,
		networkPaths:          make([]NetworkPath, 0),
		sendMessageQueue:      make(PriorityQueue, 0),
		toDeliverMessageQueue: make(map[types.ReplicaAddress][]types.Message, 0),
	}
}

func (network *Network) buildNetworkPaths(replicas []*raft.Raft) {
	paths := make([]NetworkPath, 0)

	for _, fromReplica := range replicas {
		for _, toReplica := range replicas {
			if fromReplica == toReplica {
				continue
			}

			paths = append(paths, newNetworkPath(fromReplica.ReplicaAddress(), toReplica.ReplicaAddress()))
		}
	}

	network.networkPaths = paths
}

func (network *Network) Send(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.Message) {

	messageToSend := &MessageToSend{
		AfterTick:          network.randomDelay(),
		FromReplicaAddress: fromReplicaAddress,
		ToReplicaAddress:   toReplicaAddress,
		Message:            message,
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
			network.networkPaths[i].makeReachableAfterTick = network.rand.GenBetween(0, network.config.MaxNetworkPathClogTicks)
		}
	}

	for len(network.sendMessageQueue) > 0 {
		oldestMessage := network.sendMessageQueue.Pop().(*MessageToSend)
		if oldestMessage.AfterTick > network.ticks {
			network.sendMessageQueue.Push(oldestMessage)
			return
		}

		networkPath := network.findPath(oldestMessage.FromReplicaAddress, oldestMessage.ToReplicaAddress)
		if networkPath.makeReachableAfterTick > network.ticks {
			network.sendMessageQueue.Push(oldestMessage)
			return
		}

		shouldDrop := network.rand.GenBool(network.config.DropMessageProbability)
		if shouldDrop {
			continue
		}

		if oldestMessage.AfterTick < network.ticks {
			if network.toDeliverMessageQueue[oldestMessage.ToReplicaAddress] == nil {
				network.toDeliverMessageQueue[oldestMessage.ToReplicaAddress] = make([]types.Message, 0)
			}
			network.toDeliverMessageQueue[oldestMessage.ToReplicaAddress] = append(network.toDeliverMessageQueue[oldestMessage.ToReplicaAddress], oldestMessage.Message)
		}

		shouldReplay := network.rand.GenBool(network.config.MessageReplayProbability)
		if shouldReplay {
			network.sendMessageQueue.Push(oldestMessage)
		}
	}
}

func (network *Network) findPath(fromReplicaAddress, toReplicaAddress types.ReplicaAddress) NetworkPath {
	for _, path := range network.networkPaths {
		if path.fromReplicaAddress == fromReplicaAddress && path.toReplicaAddress == toReplicaAddress {
			return path
		}
	}
	panic("unreachable")
}

func (network *Network) Receive(replicaAddress types.ReplicaAddress) (types.Message, error) {
	messages := network.toDeliverMessageQueue[replicaAddress]
	if len(messages) == 0 {
		return nil, nil
	}

	message := messages[0]
	network.toDeliverMessageQueue[replicaAddress] = messages[1:]

	return message, nil
}
