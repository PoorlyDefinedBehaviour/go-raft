package messagebus

import (
	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type MessageBus struct {
	tick     uint64
	network  types.Network
	callback types.MessageCallback
}

func NewMessageBus(network types.Network) *MessageBus {
	return &MessageBus{tick: 0, network: network}
}

// TODO: since each replica has its own bus, there's no need to include the `from` replica id.
func (bus *MessageBus) Send(from, to types.ReplicaID, message types.Message) {
	assert.True(message.ID() != 0, "message id is required")
	assert.True(from != to, "replica cannot send message to itself")

	bus.network.Send(from, to, message)
}

func (bus *MessageBus) ClientRequest(message *types.UserRequestInput) {
	bus.callback(0, message)
}

func (bus *MessageBus) RegisterOnMessageCallback(replica types.ReplicaID, callback types.MessageCallback) {
	bus.callback = callback
	bus.network.RegisterCallback(replica, callback)
}
