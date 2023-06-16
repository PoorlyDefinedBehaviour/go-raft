package messagebus

import (
	"fmt"
	"sync"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type MessageBus struct {
	lock             *sync.Mutex
	tick             uint64
	network          types.Network
	userRequestQueue []*types.UserRequestInput
}

func NewMessageBus(network types.Network) *MessageBus {
	return &MessageBus{lock: &sync.Mutex{}, tick: 0, network: network, userRequestQueue: make([]*types.UserRequestInput, 0)}
}

func (bus *MessageBus) Tick() {
	bus.tick++
}

func (messageBus *MessageBus) QueueUserRequest(request *types.UserRequestInput) {
	messageBus.lock.Lock()
	defer messageBus.lock.Unlock()

	messageBus.userRequestQueue = append(messageBus.userRequestQueue, request)
}

func (messageBus *MessageBus) RequestVote(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.RequestVoteInput) {
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")

	messageBus.lock.Lock()
	defer messageBus.lock.Unlock()

	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) SendRequestVoteResponse(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.RequestVoteOutput) {
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")

	messageBus.lock.Lock()
	defer messageBus.lock.Unlock()

	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) SendAppendEntriesRequest(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.AppendEntriesInput) {
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")
	assert.True(message.LeaderID > 0, "leader id is required")

	messageBus.lock.Lock()
	defer messageBus.lock.Unlock()

	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) SendAppendEntriesResponse(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.AppendEntriesOutput) {
	assert.True(message.ReplicaID > 0, "replica id is required")
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")

	messageBus.lock.Lock()
	defer messageBus.lock.Unlock()

	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) Receive(replicaAddress types.ReplicaAddress) (types.Message, error) {
	messageBus.lock.Lock()
	defer messageBus.lock.Unlock()

	if messageBus.tick%2 == 0 {
		if len(messageBus.userRequestQueue) > 0 {
			message := messageBus.userRequestQueue[0]
			messageBus.userRequestQueue = messageBus.userRequestQueue[1:]
			return message, nil
		}
		message, err := messageBus.network.Receive(replicaAddress)
		if err != nil {
			return nil, fmt.Errorf("receiving from network: %w", err)
		}
		return message, nil
	}

	message, err := messageBus.network.Receive(replicaAddress)
	if err != nil {
		return nil, fmt.Errorf("receiving from network: %w", err)
	}
	if message != nil {
		return message, nil
	}

	if len(messageBus.userRequestQueue) > 0 {
		message := messageBus.userRequestQueue[0]
		messageBus.userRequestQueue = messageBus.userRequestQueue[1:]
		return message, nil
	}

	return nil, nil
}
