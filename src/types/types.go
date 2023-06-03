package types

type ReplicaID = uint16
type ReplicaAddress = string

type Message interface {
	Message()
}

type RequestVoteInput struct {
	CandidateID           uint16
	CandidateTerm         uint64
	CandidateLastLogIndex uint64
	CandidateLastLogTerm  uint64
}

func (*RequestVoteInput) Message() {
	panic("unimplemented")
}

type RequestVoteOutput struct {
	CurrentTerm  uint64
	VotedGranted bool
}

func (*RequestVoteOutput) Message() {
	panic("unimplemented")
}

type Network interface {
	Receive(replicaAddress ReplicaAddress) (Message, error)
	Send(fromReplicaAddress, toReplicaAddress ReplicaAddress, message Message)
}
