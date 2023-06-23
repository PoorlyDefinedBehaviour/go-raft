package messagebus

import (
	"fmt"
	"sync"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/constants"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type MessageBus struct {
	lock    *sync.Mutex
	tick    uint64
	network types.Network
}

func NewMessageBus(network types.Network) *MessageBus {
	return &MessageBus{lock: &sync.Mutex{}, tick: 0, network: network}
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

func (bus *MessageBus) Send(from, to types.ReplicaID, message types.Message) {
	assert.True(message.ID() != 0, "message id is required")
	assert.True(from != to, "replica cannot send message to itself")

	bus.lock.Lock()
	defer bus.lock.Unlock()

	bus.network.Send(from, to, message)
}

func (bus *MessageBus) RegisterOnMessageCallback(replica types.ReplicaID, callback types.MessageCallback) {
	bus.network.RegisterCallback(replica, callback)
}
