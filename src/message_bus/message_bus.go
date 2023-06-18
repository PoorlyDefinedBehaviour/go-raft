package messagebus

import (
	"fmt"
	"sync"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/constants"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type MessageBus struct {
	lock             *sync.Mutex
	tick             uint64
	network          types.Network
	userRequestQueue map[types.ReplicaAddress][]*types.UserRequestInput
}

func NewMessageBus(network types.Network) *MessageBus {
	return &MessageBus{lock: &sync.Mutex{}, tick: 0, network: network, userRequestQueue: make(map[string][]*types.UserRequestInput, 0)}
}

func (bus *MessageBus) debug(template string, args ...interface{}) {
	message := fmt.Sprintf(template, args...)

	message = fmt.Sprintf("BUS: %s\n",
		message,
	)

	if constants.Debug {
		fmt.Println(message)
	}
}

func (bus *MessageBus) Tick() {
	bus.tick++
}

func (bus *MessageBus) QueueUserRequest(replicaAddress types.ReplicaAddress, request *types.UserRequestInput) {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	bus.debug("QUEUE USER REQUEST REPLICA=%s", replicaAddress)
	if bus.userRequestQueue[replicaAddress] == nil {
		bus.userRequestQueue[replicaAddress] = make([]*types.UserRequestInput, 0)
	}
	bus.userRequestQueue[replicaAddress] = append(bus.userRequestQueue[replicaAddress], request)
}

func (bus *MessageBus) RequestVote(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.RequestVoteInput) {
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")

	bus.lock.Lock()
	defer bus.lock.Unlock()

	bus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (bus *MessageBus) SendRequestVoteResponse(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.RequestVoteOutput) {
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")

	bus.lock.Lock()
	defer bus.lock.Unlock()

	bus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (bus *MessageBus) SendAppendEntriesRequest(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.AppendEntriesInput) {
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")
	assert.True(message.LeaderID > 0, "leader id is required")

	bus.lock.Lock()
	defer bus.lock.Unlock()

	bus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (bus *MessageBus) SendAppendEntriesResponse(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.AppendEntriesOutput) {
	assert.True(message.ReplicaID > 0, "replica id is required")
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")

	bus.lock.Lock()
	defer bus.lock.Unlock()

	bus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (bus *MessageBus) Receive(replicaAddress types.ReplicaAddress) (types.Message, error) {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	message, err := bus.network.Receive(replicaAddress)
	if err != nil {
		return nil, fmt.Errorf("receiving from network: %w", err)
	}
	if message != nil {
		return message, nil
	}

	if len(bus.userRequestQueue[replicaAddress]) > 0 {
		bus.debug("RECV USER REQUEST REPLICA=%s", replicaAddress)
		message := bus.userRequestQueue[replicaAddress][0]
		bus.userRequestQueue[replicaAddress] = bus.userRequestQueue[replicaAddress][1:]
		return message, nil
	}

	return nil, nil
}
