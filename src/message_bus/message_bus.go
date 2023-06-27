package messagebus

import (
	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type MessageBus struct {
	tick     uint64
	network  types.Network
	replica  types.ReplicaID
	callback types.MessageCallback
}

func NewMessageBus(network types.Network) *MessageBus {
	return &MessageBus{tick: 0, network: network}
}

func (bus *MessageBus) Send(to types.ReplicaID, message types.Message) {
	assert.True(message.ID() != 0, "message id is required")
	assert.True(bus.replica != to, "replica cannot send message to itself")

	bus.network.Send(bus.replica, to, message)
}

func (bus *MessageBus) ClientRequest(message *types.UserRequestInput) {
	bus.callback(0, message)
}

func (bus *MessageBus) RegisterOnMessageCallback(replica types.ReplicaID, callback types.MessageCallback) {
	assert.True(replica > 0, "replica id is required")

	bus.replica = replica
	bus.callback = callback
	bus.network.RegisterCallback(replica, callback)
}
