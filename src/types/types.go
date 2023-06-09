package types

type ReplicaID = uint16
type ReplicaAddress = string

type Message interface {
	Message()
}

type UserRequestInput struct {
	Type   uint8
	Value  []byte
	DoneCh chan error
}

func (*UserRequestInput) Message() {
	panic("unimplemented")
}

type AppendEntriesInput struct {
	LeaderID          ReplicaID
	LeaderTerm        uint64
	LeaderCommitIndex uint64
	PreviousLogIndex  uint64
	PreviousLogTerm   uint64
	Entries           []Entry
}

type Entry struct {
	Term  uint64
	Type  uint8
	Value []byte
}

func (*AppendEntriesInput) Message() {
	panic("unimplemented")
}

type AppendEntriesOutput struct {
	CurrentTerm      uint64
	Success          bool
	PreviousLogIndex uint64
	PreviousLogTerm  uint64
}

func (*AppendEntriesOutput) Message() {
	panic("unimplemented")
}

type RequestVoteInput struct {
	CandidateID           ReplicaID
	CandidateTerm         uint64
	CandidateLastLogIndex uint64
	CandidateLastLogTerm  uint64
}

func (*RequestVoteInput) Message() {
	panic("unimplemented")
}

type RequestVoteOutput struct {
	ReplicaID   ReplicaID
	CurrentTerm uint64
	VoteGranted bool
}

func (*RequestVoteOutput) Message() {
	panic("unimplemented")
}

type Network interface {
	Receive(replicaAddress ReplicaAddress) (Message, error)
	Send(fromReplicaAddress, toReplicaAddress ReplicaAddress, message Message)
}

type StateMachine interface {
	Apply(*Entry) error
}
