package simulator_test

import (
	"fmt"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type MessageBus struct {
	network *Network
}

func NewMessageBus(network *Network) *MessageBus {
	return &MessageBus{network: network}
}

func (messageBus *MessageBus) RequestVote(targetReplicaAddress string, input types.RequestVoteInput) {
	fmt.Printf("\n\naaaaaaa targetReplicaAddress %+v\n\n", targetReplicaAddress)
	fmt.Printf("\n\naaaaaaa input %+v\n\n", input)
	// messageBus.network.sendRpc(targetReplicaAddress, []byte("todo"))
}

func (messageBus *MessageBus) SendRequestVoteResponse(targetReplicaAddress string, input types.RequestVoteOutput) {
	// messageBus.network.sendRpc(targetReplicaAddress, []byte("todo"))
}

func (messageBus *MessageBus) Receive() types.Message {
	return nil
}
