package network

import (
	"fmt"

	"github.com/poorlydefinedbehaviour/raft-go/src/constants"
	"github.com/poorlydefinedbehaviour/raft-go/src/mapx"
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

	// The network path from replica A to replica B.
	networkPaths []NetworkPath

	// Messages that need to be sent to a replica.
	sendMessageQueue PriorityQueue
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
	CanBeDeliveredAtTick uint64
	FromReplicaAddress   types.ReplicaAddress
	ToReplicaAddress     types.ReplicaAddress
	Message              types.Message
	Callback             types.MessageCallback
	Index                int
}

func New(config NetworkConfig, rand rand.Random) *Network {
	return &Network{
		config:           config,
		rand:             rand,
		sendMessageQueue: make(PriorityQueue, 0),
	}
}

func buildNetworkPaths(replicasAddresses []types.ReplicaAddress) []NetworkPath {
	paths := make([]NetworkPath, 0)

	for _, fromReplica := range replicasAddresses {
		for _, toReplica := range replicasAddresses {
			if fromReplica == toReplica {
				continue
			}

			paths = append(paths, newNetworkPath(fromReplica, toReplica))
		}
	}

	return paths
}

func (network *Network) Setup(replicasOnMessage map[types.ReplicaAddress]types.MessageCallback) {
	network.networkPaths = buildNetworkPaths(mapx.Keys(replicasOnMessage))
}

func (network *Network) debug(template string, args ...interface{}) {
	message := fmt.Sprintf(template, args...)

	message = fmt.Sprintf("NETWORK: TICK=%d %s\n",
		network.ticks,
		message,
	)

	if constants.Debug {
		fmt.Println(message)
	}
}

func (network *Network) MessagesFromTo(from, to types.ReplicaAddress) []types.Message {
	messages := make([]types.Message, 0)

	// Messages that may be delivered in the future.
	for _, message := range network.sendMessageQueue {
		if message.FromReplicaAddress == from && message.ToReplicaAddress == to {
			messages = append(messages, message.Message)
		}
	}

	return messages
}

func (network *Network) Send(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.Message, callback types.MessageCallback) {
	switch message.(type) {
	case *types.UserRequestInput:
		network.debug("SEND UserRequestInput %s -> %s", fromReplicaAddress, toReplicaAddress)
	case *types.AppendEntriesInput:
		network.debug("SEND AppendEntriesInput %s -> %s", fromReplicaAddress, toReplicaAddress)
	case *types.AppendEntriesOutput:
		network.debug("SEND AppendEntriesOutput %s -> %s", fromReplicaAddress, toReplicaAddress)
	case *types.RequestVoteInput:
		network.debug("SEND RequestVoteInput %s -> %s", fromReplicaAddress, toReplicaAddress)
	case *types.RequestVoteOutput:
		network.debug("SEND RequestVoteOutput %s -> %s", fromReplicaAddress, toReplicaAddress)
	default:
		panic(fmt.Sprintf("unexpected message type: %+v", message))
	}
	messageToSend := &MessageToSend{
		CanBeDeliveredAtTick: network.randomDelay(),
		FromReplicaAddress:   fromReplicaAddress,
		ToReplicaAddress:     toReplicaAddress,
		Message:              message,
		Callback:             callback,
	}

	network.sendMessageQueue.Push(messageToSend)
}

func (network *Network) randomDelay() uint64 {
	return network.ticks + network.rand.GenBetween(0, network.config.MaxMessageDelayTicks)
}

// Returns `true` when there are messages in the network that will be
// delivered some time in the future.
func (network *Network) HasPendingMessages() bool {
	return len(network.sendMessageQueue) > 0
}

func (network *Network) Tick() {
	network.ticks++

	for i := range network.networkPaths {
		shouldMakeUnreachable := network.rand.GenBool(network.config.PathClogProbability)
		if shouldMakeUnreachable {
			network.debug("UNREACHABLE UNTIL_TICK=%d %s -> %s",
				network.networkPaths[i].makeReachableAfterTick,
				network.networkPaths[i].fromReplicaAddress,
				network.networkPaths[i].toReplicaAddress,
			)
			network.networkPaths[i].makeReachableAfterTick = network.rand.GenBetween(0, network.config.MaxNetworkPathClogTicks)
		}
	}

	for len(network.sendMessageQueue) > 0 {
		oldestMessage := network.sendMessageQueue.Pop().(*MessageToSend)

		if oldestMessage.CanBeDeliveredAtTick > network.ticks {
			network.sendMessageQueue.Push(oldestMessage)
			return
		}

		networkPath := network.findPath(oldestMessage.FromReplicaAddress, oldestMessage.ToReplicaAddress)
		if networkPath.makeReachableAfterTick > network.ticks {
			network.sendMessageQueue.Push(oldestMessage)
			continue
		}

		shouldDrop := network.rand.GenBool(network.config.DropMessageProbability)
		if shouldDrop {
			network.debug("DROP MESSAGE=%+v", oldestMessage)
			continue
		}

		network.debug("DELIVER MESSAGE=%+v", oldestMessage)
		oldestMessage.Callback(oldestMessage.FromReplicaAddress, oldestMessage.Message)

		shouldReplay := network.rand.GenBool(network.config.MessageReplayProbability)
		if shouldReplay {
			network.debug("REPLAY MESSAGE=%+v", oldestMessage)
			oldestMessage.Callback(oldestMessage.FromReplicaAddress, oldestMessage.Message)
		}
	}
}

func (network *Network) findPath(fromReplicaAddress, toReplicaAddress types.ReplicaAddress) NetworkPath {
	for _, path := range network.networkPaths {
		if path.fromReplicaAddress == fromReplicaAddress && path.toReplicaAddress == toReplicaAddress {
			return path
		}
	}

	panic(fmt.Sprintf("unreachable: didn't find path. fromReplicaAddress=%s toReplicaAddress=%s networkPaths=%+v", fromReplicaAddress, toReplicaAddress, network.networkPaths))
}
