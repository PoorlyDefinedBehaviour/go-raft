package network

import (
	"container/heap"
	"fmt"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
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

	callbacks map[types.ReplicaID]types.MessageCallback

	// The network path from replica A to replica B.
	networkPaths []NetworkPath

	// Messages that need to be sent to a replica.
	sendMessageQueue PriorityQueue
}

type NetworkPath struct {
	from                   types.ReplicaID
	to                     types.ReplicaID
	makeReachableAfterTick uint64
}

func newNetworkPath(from, to types.ReplicaID) NetworkPath {
	return NetworkPath{
		from:                   from,
		to:                     to,
		makeReachableAfterTick: 0,
	}
}

type MessageToSend struct {
	CanBeDeliveredAtTick uint64
	From                 types.ReplicaID
	To                   types.ReplicaID
	Message              types.Message
	Index                int
}

func New(config NetworkConfig, rand rand.Random) *Network {
	return &Network{
		config:           config,
		rand:             rand,
		sendMessageQueue: make(PriorityQueue, 0),
	}
}

func buildNetworkPaths(replicasAddresses []types.ReplicaID) []NetworkPath {
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

func (network *Network) Setup(replicasOnMessage map[types.ReplicaID]types.MessageCallback) {
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

func (network *Network) RegisterCallback(replica types.ReplicaID, callback types.MessageCallback) {
	if network.callbacks == nil {
		network.callbacks = make(map[types.ReplicaID]types.MessageCallback)
	}
	network.callbacks[replica] = callback
}

func (network *Network) MessagesFromTo(from, to types.ReplicaID) []types.Message {
	messages := make([]types.Message, 0)

	// Messages that may be delivered in the future.
	for _, message := range network.sendMessageQueue {
		if message.From == from && message.To == to {
			messages = append(messages, message.Message)
		}
	}

	return messages
}

func (network *Network) Send(from, to types.ReplicaID, message types.Message) {
	assert.True(from != to, "replica cannot send message to itself")

	switch message.(type) {
	case *types.UserRequestInput:
		network.debug("SEND UserRequestInput %d -> %d", from, to)
	case *types.AppendEntriesInput:
		network.debug("SEND AppendEntriesInput %d -> %d", from, to)
	case *types.AppendEntriesOutput:
		network.debug("SEND AppendEntriesOutput %d -> %d", from, to)
	case *types.RequestVoteInput:
		network.debug("SEND RequestVoteInput %d -> %d", from, to)
	case *types.RequestVoteOutput:
		network.debug("SEND RequestVoteOutput %d -> %d", from, to)
	default:
		panic(fmt.Sprintf("unexpected message type: %+v", message))
	}

	messageToSend := &MessageToSend{
		CanBeDeliveredAtTick: network.randomDelay(),
		From:                 from,
		To:                   to,
		Message:              message,
	}

	heap.Push(&network.sendMessageQueue, messageToSend)
}

func (network *Network) randomDelay() uint64 {
	return network.ticks + network.rand.GenBetween(0, network.config.MaxMessageDelayTicks) + 1
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
			network.networkPaths[i].makeReachableAfterTick = network.rand.GenBetween(0, network.config.MaxNetworkPathClogTicks)
			network.debug("UNREACHABLE UNTIL_TICK=%d %d -> %d",
				network.networkPaths[i].makeReachableAfterTick,
				network.networkPaths[i].from,
				network.networkPaths[i].to,
			)
		}
	}

	for len(network.sendMessageQueue) > 0 {
		oldestMessage := heap.Pop(&network.sendMessageQueue).(*MessageToSend)

		if oldestMessage.CanBeDeliveredAtTick > network.ticks {
			network.sendMessageQueue.Push(oldestMessage)
			return
		}

		networkPath := network.findPath(oldestMessage.From, oldestMessage.To)
		if networkPath.makeReachableAfterTick > network.ticks {
			network.debug("UNREACHABLE(%d -> %d) DROP MESSAGE=%+v", networkPath.from, networkPath.to, oldestMessage)
			continue
		}

		shouldDrop := network.rand.GenBool(network.config.DropMessageProbability)
		if shouldDrop {
			network.debug("DROP MESSAGE=%+v", oldestMessage)
			continue
		}

		callback := network.callbacks[oldestMessage.To]

		network.debug("DELIVER MESSAGE=%+v", oldestMessage)
		callback(oldestMessage.From, oldestMessage.Message)

		shouldReplay := network.rand.GenBool(network.config.MessageReplayProbability)
		if shouldReplay {
			network.debug("REPLAY MESSAGE=%+v", oldestMessage)
			network.Send(oldestMessage.From, oldestMessage.To, oldestMessage.Message)
		}
	}
}

func (network *Network) findPath(from, to types.ReplicaID) NetworkPath {
	for _, path := range network.networkPaths {
		if path.from == from && path.to == to {
			return path
		}
	}

	panic(fmt.Sprintf("unreachable: didn't find path. from=%d to=%d networkPaths=%+v", from, to, network.networkPaths))
}
