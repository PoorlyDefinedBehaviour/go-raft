package messagebus

import (
	"fmt"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type MessageBus struct {
	network types.Network
}

func NewMessageBus(network types.Network) *MessageBus {
	return &MessageBus{network: network}
}

func (messageBus *MessageBus) RequestVote(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.RequestVoteInput) {
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")

	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) SendRequestVoteResponse(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.RequestVoteOutput) {
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")

	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) SendAppendEntriesRequest(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.AppendEntriesInput) {
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")

	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) SendAppendEntriesResponse(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.AppendEntriesOutput) {
	assert.True(fromReplicaAddress != toReplicaAddress, "replica cannot send message to itself")

	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) Receive(replicaAddress types.ReplicaAddress) (types.Message, error) {
	message, err := messageBus.network.Receive(replicaAddress)
	if err != nil {
		return nil, fmt.Errorf("receiving from network: %w", err)
	}

	return message, nil
}
