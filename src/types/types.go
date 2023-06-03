package types

type ReplicaID = uint16

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
