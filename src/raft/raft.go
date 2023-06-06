package raft

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

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
	messageBus *messagebus.MessageBus

	// The state mutated directly by the Raft struct.
	mutableState MutableState

	// The state machine being built on top of raft.
	stateMachine types.StateMachine

	storage storage.Storage
}

type MutableState struct {
	// The current tick.
	currentTick uint64

	// State that is reset every new term.
	currentTermState CurrentTermState

	// Index that has been committed by the leader.
	commitIndex uint64

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

	// The address of this replica.
	ReplicaAddress types.ReplicaAddress

	// How long to wait for without receiving a heartbeat from the leader to start an election.
	LeaderElectionTimeout time.Duration

	// The list of other replicas in the cluster. Should not include this replica.
	Replicas []Replica
}

type Replica struct {
	ReplicaID      types.ReplicaID
	ReplicaAddress types.ReplicaAddress
}

func NewRaft(config Config, messageBus *messagebus.MessageBus, storage storage.Storage, stateMachine types.StateMachine) (*Raft, error) {
	if config.ReplicaID == 0 {
		return nil, fmt.Errorf("replica id cannot be 0")
	}
	if config.ReplicaAddress == "" {
		return nil, fmt.Errorf("replica address is required")
	}
	if config.LeaderElectionTimeout == 0 {
		return nil, fmt.Errorf("leader election timeout is required")
	}

	// Ensure the replica itself is not in the replica list.
	config.Replicas = removeByReplicaID(config.Replicas, config.ReplicaID)

	raft := &Raft{
		config:       config,
		messageBus:   messageBus,
		storage:      storage,
		stateMachine: stateMachine,
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

	return raft, nil
}

func removeByReplicaID(replicas []Replica, replicaID types.ReplicaID) []Replica {
	out := make([]Replica, 0, len(replicas))

	for _, replica := range replicas {
		if replica.ReplicaID != replicaID {
			out = append(out, replica)
		}
	}

	return out
}

func (raft *Raft) ReplicaAddress() types.ReplicaAddress {
	return raft.config.ReplicaAddress
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

func (raft *Raft) VotedForCandidateInCurrentTerm(candidateID uint16) bool {
	return raft.mutableState.currentTermState.votedFor == candidateID
}

func (raft *Raft) resetElectionTimeout() {
	raft.mutableState.nextLeaderElectionTimeout = raft.mutableState.nextLeaderElectionTimeout + uint64(raft.config.LeaderElectionTimeout.Milliseconds())
}

func (raft *Raft) startElection() {
	assert.True(raft.mutableState.currentTermState.votedFor == 0, "cannot have voted for someone and be in the candidate state")

	if raft.mutableState.currentTermState.electionStarted {
		return
	}

	log.Printf("replica=%s starting election\n", raft.ReplicaAddress())

	raft.newTerm()

	for _, replica := range raft.config.Replicas {
		raft.messageBus.RequestVote(raft.ReplicaAddress(), replica.ReplicaAddress, types.RequestVoteInput{
			CandidateTerm:         raft.mutableState.currentTermState.term,
			CandidateID:           raft.config.ReplicaID,
			CandidateLastLogIndex: 1,
			CandidateLastLogTerm:  1,
		})
	}

	raft.mutableState.currentTermState.electionStarted = true
}

type newTermOption = func(*CurrentTermState)

func withTerm(term uint64) newTermOption {
	return func(currentTermState *CurrentTermState) {
		currentTermState.term = term
	}
}

func (raft *Raft) newTerm(options ...newTermOption) error {
	newState := CurrentTermState{
		term:          raft.mutableState.currentTermState.term + 1,
		votedFor:      0,
		votesReceived: 0,
	}
	for _, option := range options {
		option(&newState)
	}

	if err := raft.storage.Persist(storage.State{CurrentTerm: newState.term, VotedFor: newState.votedFor}); err != nil {
		return fmt.Errorf("persisting term and voted for: %w", err)
	}

	raft.mutableState.currentTermState = newState

	log.Printf("replica=%s term=%d changed to new term\n", raft.ReplicaAddress(), raft.mutableState.currentTermState.term)

	return nil
}

func (raft *Raft) voteFor(candidateID types.ReplicaID, candidateTerm uint64) {
	raft.newTerm(withTerm(candidateTerm))
	raft.mutableState.currentTermState.votedFor = candidateID
	raft.transitionToState(Follower)
	log.Printf("replica=%s term=%d candidate=%d voted for candidate\n", raft.ReplicaAddress(), candidateID, raft.mutableState.currentTermState.term)
}

func (raft *Raft) transitionToState(state State) {
	raft.mutableState.state = state

	switch raft.mutableState.state {
	case Leader:
		log.Printf("replica=%s transitioning to leader\n", raft.ReplicaAddress())
	case Candidate:
		log.Printf("replica=%s transitioning to candidate\n", raft.ReplicaAddress())
	case Follower:
		log.Printf("replica=%s transitioning to follower\n", raft.ReplicaAddress())
		raft.resetElectionTimeout()
	default:
		panic(fmt.Sprintf("unexpected state: %d", raft.mutableState.state))
	}
}

func (raft *Raft) handleMessages() {
	message, err := raft.messageBus.Receive(raft.ReplicaAddress())
	if message == nil {
		return
	}
	if err != nil {
		log.Printf("error receiving message from message bus: %s", err)
		return
	}

	switch message := message.(type) {
	// Invoked by leader to replicate log entries (§5.3); also used as
	// heartbeat (§5.2).
	//
	// Arguments:
	// term leader’s term
	// leaderId so follower can redirect clients
	// prevLogIndex index of log entry immediately preceding
	// new ones
	// prevLogTerm term of prevLogIndex entry
	// entries[] log entries to store (empty for heartbeat;
	// may send more than one for efficiency)
	// leaderCommit leader’s commitIndex
	//
	// Results:
	// term currentTerm, for leader to update itself
	// success true if follower contained entry matching
	// prevLogIndex and prevLogTerm
	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex to min(leaderCommit, lastLogIndex)
	case *types.AppendEntriesInput:
		log.Printf("replica=%s message=%+v handling AppendEntries\n", raft.ReplicaAddress(), message)

		replica := findReplicaByID(raft.config.Replicas, message.LeaderID)

		appendEntriesOutput := types.AppendEntriesOutput{
			CurrentTerm:      raft.mutableState.currentTermState.term,
			Success:          false,
			PreviousLogIndex: raft.storage.LastLogIndex(),
			PreviousLogTerm:  raft.storage.LastLogTerm(),
		}

		if message.LeaderTerm < raft.mutableState.currentTermState.term {
			raft.messageBus.SendAppendEntriesResponse(raft.config.ReplicaAddress, replica.ReplicaAddress, appendEntriesOutput)
			return
		}

		if message.PreviousLogIndex != raft.storage.LastLogIndex() {
			raft.messageBus.SendAppendEntriesResponse(raft.config.ReplicaAddress, replica.ReplicaAddress, appendEntriesOutput)
			return
		}

		entry, err := raft.storage.GetEntryAtIndex(message.PreviousLogIndex)
		if err != nil && !errors.Is(err, storage.ErrIndexOutOfBounds) {
			panic("todo")
		}

		success := true

		if entry != nil && entry.Term != message.PreviousLogTerm {
			success = false
			if err := raft.storage.TruncateLogStartingFrom(message.PreviousLogIndex); err != nil {
				panic(fmt.Sprintf("TODO: %s", err))
			}
		}

		if err := raft.storage.AppendEntries(message.Entries); err != nil {
			panic("todo")
		}

		if message.LeaderCommitIndex > raft.mutableState.commitIndex {
			raft.applyUncommittedEntries(message.LeaderCommitIndex, raft.storage.LastLogIndex())
		}

		raft.mutableState.currentTermState.term = message.LeaderTerm

		raft.messageBus.SendAppendEntriesResponse(raft.config.ReplicaAddress, replica.ReplicaAddress, types.AppendEntriesOutput{
			CurrentTerm:      raft.mutableState.currentTermState.term,
			Success:          success,
			PreviousLogIndex: raft.storage.LastLogIndex(),
			PreviousLogTerm:  raft.storage.LastLogTerm(),
		})

	case *types.RequestVoteInput:
		log.Printf("replica=%s message=%+v handling RequestVote\n", raft.ReplicaAddress(), message)

		replica := findReplicaByID(raft.config.Replicas, message.CandidateID)
		// Receiver implementation:
		// 1. Reply false if term < currentTerm (§5.1)
		// 2. If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		if message.CandidateTerm < raft.mutableState.currentTermState.term {
			log.Printf("replica=%s candidateTerm=%d replicaTerm=%d candidate term is less than replica term, not granting vote\n", raft.ReplicaAddress(), message.CandidateTerm, raft.mutableState.currentTermState.term)
			raft.messageBus.SendRequestVoteResponse(raft.config.ReplicaAddress, replica.ReplicaAddress, types.RequestVoteOutput{
				CurrentTerm: raft.mutableState.currentTermState.term,
				VoteGranted: false,
			})
			return
		}

		alreadyVotedForCandidate := raft.hasVotedForCandidate(message.CandidateID)
		hasVoted := raft.hasVoted()
		candidateLogUpToDate := raft.isCandidateLogUpToDate(message)
		voteGranted := alreadyVotedForCandidate || (!hasVoted && candidateLogUpToDate)

		log.Printf("replica=%s voteGranted=%v alreadyVotedForCandidate=%v hasVoted=%v candidateLogUpToDate=%v\n",
			raft.ReplicaAddress(),
			voteGranted,
			alreadyVotedForCandidate,
			hasVoted,
			candidateLogUpToDate,
		)

		if voteGranted {
			raft.voteFor(message.CandidateID, message.CandidateTerm)
		}

		raft.messageBus.SendRequestVoteResponse(raft.ReplicaAddress(), replica.ReplicaAddress, types.RequestVoteOutput{
			CurrentTerm: raft.mutableState.currentTermState.term,
			VoteGranted: voteGranted,
		})

	case *types.RequestVoteOutput:
		log.Printf("replica=%s message=%+v handling RequestVoteOutput\n", raft.ReplicaAddress(), message)

		if message.CurrentTerm > raft.mutableState.currentTermState.term {
			raft.newTerm(withTerm(message.CurrentTerm))
			raft.transitionToState(Follower)
			return
		}

		if message.VoteGranted {
			raft.mutableState.currentTermState.votesReceived++

			if raft.mutableState.currentTermState.votesReceived >= raft.majority() {
				raft.transitionToState(Leader)
			}
		}
	default:
		panic(fmt.Sprintf("unexpected message: %+v", message))
	}
}

func (raft *Raft) leaderElectionTimeoutFired() bool {
	return raft.mutableState.currentTick > raft.mutableState.nextLeaderElectionTimeout
}

func (raft *Raft) isCandidateLogUpToDate(input *types.RequestVoteInput) bool {
	upToDate := input.CandidateTerm >= raft.mutableState.currentTermState.term &&
		input.CandidateLastLogTerm >= raft.storage.LastLogTerm() &&
		input.CandidateLastLogIndex >= raft.storage.LastLogIndex()

	if !upToDate {
		log.Printf("replica=%s candidateID=%d candidateLastLogTerm=%d candidateLastLogIndex=%d candidate log is not up to date\n",
			raft.ReplicaAddress(),
			input.CandidateID,
			input.CandidateLastLogIndex,
			input.CandidateLastLogTerm,
		)
	}

	return upToDate
}

func (raft *Raft) hasVotedForCandidate(candidateID types.ReplicaID) bool {
	return raft.mutableState.currentTermState.votedFor == candidateID
}

func (raft *Raft) hasVoted() bool {
	return raft.mutableState.currentTermState.votedFor != 0
}

func (raft *Raft) applyUncommittedEntries(leaderCommitIndex uint64, lastlogIndex uint64) error {
	commitIndex := leaderCommitIndex
	if lastlogIndex < leaderCommitIndex {
		commitIndex = lastlogIndex
	}
	for i := raft.mutableState.commitIndex; i < commitIndex; i++ {
		entry, err := raft.storage.GetEntryAtIndex(i)
		if err != nil {
			return fmt.Errorf("fetching entry at index: index=%d %w", i, err)
		}
		if err := raft.stateMachine.Apply(entry); err != nil {
			return fmt.Errorf("applying entry to state machine: %w", err)
		}
	}

	raft.mutableState.commitIndex = commitIndex

	return nil
}

func findReplicaByID(replicas []Replica, replicaID types.ReplicaID) Replica {
	for _, replica := range replicas {
		if replica.ReplicaID == replicaID {
			return replica
		}
	}
	panic(fmt.Sprintf("unreachable: unable to find replica with id: %d", replicaID))
}
