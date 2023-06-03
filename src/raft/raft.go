package raft

import (
	"fmt"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

type MessageBus interface {
	RequestVote(targetReplicaAddress string, input types.RequestVoteInput)
	SendRequestVoteResponse(targetReplicaAddress string, input types.RequestVoteOutput)
	Receive() types.Message
}

type State = uint8

const (
	Follower  State = 1
	Candidate State = 2
	Leader    State = 3
)

type Raft struct {
	// The replica configuration.
	config Config

	// Used to send inputs to other replicas.
	messageBus MessageBus

	// The state mutated directly by the Raft struct.
	mutableState MutableState
}

type MutableState struct {
	// The current tick.
	currentTick uint64

	// State that is reset every new term.
	currentTermState CurrentTermState

	// The current state of this replica.
	state State

	// When the next leader election timeout will fire.
	nextLeaderElectionTimeout uint64
}

type CurrentTermState struct {
	// The current term of this replica.
	term uint64

	// The candidate this replica voted for in the current term.
	votedFor uint16

	// Has this replica started an election in this term?
	electionStarted bool

	// The number of votes received in the current election if there's one.
	votesReceived uint16
}

type Config struct {
	// The ID of this replica.
	ReplicaID types.ReplicaID

	// How long to wait for without receiving a heartbeat from the leader to start an election.
	LeaderElectionTimeout time.Duration

	// The list of other replicas in the cluste. Should not include this replica.
	Replicas []Replica
}

type Replica struct {
	Address string
}

func NewRaft(config Config, messageBus MessageBus) *Raft {
	raft := &Raft{
		config:     config,
		messageBus: messageBus,
		mutableState: MutableState{
			currentTick: 0,
			state:       Follower,
			currentTermState: CurrentTermState{
				term:          0,
				votedFor:      0,
				votesReceived: 0,
			},
			nextLeaderElectionTimeout: 0,
		},
	}

	raft.transitionToState(Follower)

	return raft
}

func (raft *Raft) ReplicaID() types.ReplicaID {
	return raft.config.ReplicaID
}

func (raft *Raft) Start() {
	for {
		select {
		case <-time.After(1 * time.Millisecond):
			raft.Tick()
		}
	}
}

func (raft *Raft) Tick() {
	raft.mutableState.currentTick++

	switch raft.mutableState.state {
	case Follower:
		if raft.leaderElectionTimeoutFired() {
			raft.transitionToState(Candidate)
			return
		}
		raft.handleMessages()
	case Candidate:
		fmt.Printf("\n\naaaaaaa  candidate\n\n")
		raft.startElection()
		raft.handleMessages()
	case Leader:
	default:
		panic(fmt.Sprintf("unexpected raft state: %d", raft.mutableState.state))
	}
}

func (raft *Raft) majority() uint16 {
	return uint16(len(raft.config.Replicas)/2 + 1)
}

func (raft *Raft) LastLogTerm() uint64 {
	panic("todo")
}

func (raft *Raft) LastLogIndex() uint64 {
	panic("todo")
}

func (raft *Raft) VotedForCandidateInCurrentTerm(candidateID uint16) bool {
	return raft.mutableState.currentTermState.votedFor == candidateID
}

func (raft *Raft) HasVotedInCurrentTerm() bool {
	return raft.mutableState.currentTermState.votedFor != 0
}

func (raft *Raft) resetElectionTimeout() {
	raft.mutableState.nextLeaderElectionTimeout = raft.mutableState.nextLeaderElectionTimeout + uint64(raft.config.LeaderElectionTimeout.Milliseconds())
}

func (raft *Raft) startElection() {
	assert.True(raft.mutableState.currentTermState.votedFor == 0, "cannot have voted for someone and be in the candidate state")
	if raft.mutableState.currentTermState.electionStarted {
		return
	}
	fmt.Println("starting election")

	raft.newTerm()

	fmt.Printf("\n\naaaaaaa raft.config.Replicas %+v\n\n", raft.config.Replicas)
	for _, replica := range raft.config.Replicas {
		raft.messageBus.RequestVote(replica.Address, types.RequestVoteInput{
			CandidateTerm:         raft.mutableState.currentTermState.term,
			CandidateID:           raft.config.ReplicaID,
			CandidateLastLogIndex: 1,
			CandidateLastLogTerm:  1,
		})
	}
}

type newTermOption = func(*CurrentTermState)

func withTerm(term uint64) newTermOption {
	return func(currentTermState *CurrentTermState) {
		currentTermState.term = term
	}
}

func (raft *Raft) newTerm(options ...newTermOption) {
	raft.mutableState.currentTermState = CurrentTermState{
		term:          raft.mutableState.currentTermState.term + 1,
		votedFor:      0,
		votesReceived: 0,
	}
	for _, option := range options {
		option(&raft.mutableState.currentTermState)
	}
}

func (raft *Raft) transitionToState(state State) {
	raft.mutableState.state = state

	switch raft.mutableState.state {
	case Leader:
		fmt.Println("transitioning to leader")
	case Candidate:
		fmt.Println("transitioning to candidate")
	case Follower:
		fmt.Println("transitioning to follower")
		raft.resetElectionTimeout()
	default:
		panic(fmt.Sprintf("unexpected state: %d", raft.mutableState.state))
	}
}

func (raft *Raft) handleMessages() {
	message := raft.messageBus.Receive()
	if message == nil {
		return
	}

	switch message := message.(type) {
	case *types.RequestVoteInput:
		// Receiver implementation:
		// 1. Reply false if term < currentTerm (§5.1)
		// 2. If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		if message.CandidateTerm < raft.mutableState.currentTermState.term {
			raft.messageBus.SendRequestVoteResponse("todo", types.RequestVoteOutput{
				CurrentTerm:  raft.mutableState.currentTermState.term,
				VotedGranted: false,
			})
			return
		}

		if message.CandidateTerm > raft.mutableState.currentTermState.term {
			raft.newTerm(withTerm(message.CandidateTerm))
			raft.transitionToState(Follower)
		}

		voteGranted :=
			raft.VotedForCandidateInCurrentTerm(message.CandidateID) ||
				(!raft.HasVotedInCurrentTerm() && raft.isCandidateLogUpToDate(message))

		raft.messageBus.SendRequestVoteResponse("todo", types.RequestVoteOutput{
			CurrentTerm:  raft.mutableState.currentTermState.term,
			VotedGranted: voteGranted,
		})

		if voteGranted {
			raft.transitionToState(Follower)
			raft.mutableState.currentTermState.votedFor = message.CandidateID
		}

	case *types.RequestVoteOutput:
		if message.CurrentTerm > raft.mutableState.currentTermState.term {
			raft.newTerm(withTerm(message.CurrentTerm))
			raft.transitionToState(Follower)
			return
		}

		if message.VotedGranted {
			raft.mutableState.currentTermState.votesReceived++

			if raft.mutableState.currentTermState.votesReceived >= raft.majority() {
				raft.transitionToState(Leader)
			}
		}
	default:
		panic(fmt.Sprintf("unexpected message: %+v", message))
	}
}

func (raft *Raft) isCandidateLogUpToDate(input *types.RequestVoteInput) bool {
	return input.CandidateTerm >= raft.mutableState.currentTermState.term &&
		input.CandidateLastLogTerm >= raft.LastLogTerm() &&
		input.CandidateLastLogIndex >= raft.LastLogIndex()
}

func (raft *Raft) leaderElectionTimeoutFired() bool {
	return raft.mutableState.currentTick > raft.mutableState.nextLeaderElectionTimeout
}
