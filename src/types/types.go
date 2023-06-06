package types

type ReplicaID = uint16
type ReplicaAddress = string

type Message interface {
	Message()
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
	Term uint64
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
	CandidateID           uint16
	CandidateTerm         uint64
	CandidateLastLogIndex uint64
	CandidateLastLogTerm  uint64
}

func (*RequestVoteInput) Message() {
	panic("unimplemented")
}

type RequestVoteOutput struct {
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
	Apply(entry *Entry) error
}
