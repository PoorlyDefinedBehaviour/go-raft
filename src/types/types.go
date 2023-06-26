package types

// Type used by raft empty heartbeat entries.
const (
	HeartbeatEntryType   = 1
	NewLeaderEntryType   = 2
	MaxReservedEntryType = 10
)

type MessageCallback = func(ReplicaID, Message)

type ReplicaID = uint16

type ReplicaAddress = string

type Message interface {
	ID() uint64
}

type UserRequestInput struct {
	MessageID uint64
	Type      uint8
	Value     []byte
	DoneCh    chan error
}

func (message *UserRequestInput) ID() uint64 {
	return message.MessageID
}

type AppendEntriesInput struct {
	MessageID         uint64
	LeaderID          ReplicaID
	LeaderTerm        uint64
	LeaderCommitIndex uint64
	PreviousLogIndex  uint64
	PreviousLogTerm   uint64
	Entries           []Entry
}

type Entry struct {
	Term  uint64
	Index uint64
	Type  uint8
	Value []byte
}

func (message *AppendEntriesInput) ID() uint64 {
	return message.MessageID
}

func (entry *Entry) IsSystemEntry() bool {
	return entry.Type == NewLeaderEntryType || entry.Type == HeartbeatEntryType
}

type AppendEntriesOutput struct {
	MessageID        uint64
	ReplicaID        ReplicaID
	CurrentTerm      uint64
	Success          bool
	PreviousLogIndex uint64
	PreviousLogTerm  uint64
}

func (message *AppendEntriesOutput) ID() uint64 {
	return message.MessageID
}

type RequestVoteInput struct {
	MessageID             uint64
	CandidateID           ReplicaID
	CandidateTerm         uint64
	CandidateLastLogIndex uint64
	CandidateLastLogTerm  uint64
}

func (message *RequestVoteInput) ID() uint64 {
	return message.MessageID
}

type RequestVoteOutput struct {
	MessageID   uint64
	ReplicaID   ReplicaID
	CurrentTerm uint64
	VoteGranted bool
}

func (message *RequestVoteOutput) ID() uint64 {
	return message.MessageID
}

type Network interface {
	RegisterCallback(replica ReplicaID, callback MessageCallback)
	MessagesFromTo(from, to ReplicaID) []Message
	Send(from, to ReplicaID, message Message)
}

type StateMachine interface {
	Apply(*Entry) error
}
