package messagebus

import (
	"fmt"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type MessageBus struct {
	network types.Network
}

func NewMessageBus(network types.Network) *MessageBus {
	return &MessageBus{network: network}
}

func (messageBus *MessageBus) RequestVote(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.RequestVoteInput) {
	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) SendRequestVoteResponse(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.RequestVoteOutput) {
	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) SendAppendEntriesRequest(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.AppendEntriesInput) {
	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) SendAppendEntriesResponse(fromReplicaAddress, toReplicaAddress types.ReplicaAddress, message types.AppendEntriesOutput) {
	messageBus.network.Send(fromReplicaAddress, toReplicaAddress, &message)
}

func (messageBus *MessageBus) Receive(replicaAddress types.ReplicaAddress) (types.Message, error) {
	message, err := messageBus.network.Receive(replicaAddress)
	if err != nil {
		return nil, fmt.Errorf("receiving from network: %w", err)
	}

	return message, nil
}
