package messagebus

import (
	"fmt"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type MessageBus struct {
	tick    uint64
	network types.Network
}

func NewMessageBus(network types.Network) *MessageBus {
	return &MessageBus{tick: 0, network: network}
}

func (bus *MessageBus) Tick() {
	bus.tick++
}

func (bus *MessageBus) Send(from, to types.ReplicaID, message types.Message) {
	fmt.Println("bus.Send()")
	assert.True(message.ID() != 0, "message id is required")
	assert.True(from != to, "replica cannot send message to itself")

	bus.network.Send(from, to, message)
}

func (bus *MessageBus) RegisterOnMessageCallback(replica types.ReplicaID, callback types.MessageCallback) {
	bus.network.RegisterCallback(replica, callback)
}
