package raft

import (
	"errors"
	"fmt"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"go.uber.org/zap"
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

	logger *zap.SugaredLogger
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

	// As a leader, when the next heartbeat should be sent to replicas.
	nextLeaderHeartbeatTimeout uint64
}

type CurrentTermState struct {
	// The current term of this replica.
	term uint64

	// The candidate this replica voted for in the current term.
	votedFor uint16

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

	// As a leader, how long to wait for before sending a heartbeat to replicas.
	LeaderHeartbeatTimeout time.Duration

	// The list of other replicas in the cluster. Should not include this replica.
	Replicas []Replica
}

type Replica struct {
	ReplicaID      types.ReplicaID
	ReplicaAddress types.ReplicaAddress
}

func NewRaft(config Config, messageBus *messagebus.MessageBus, storage storage.Storage, stateMachine types.StateMachine, logger *zap.SugaredLogger) (*Raft, error) {
	if config.ReplicaID == 0 {
		return nil, fmt.Errorf("replica id cannot be 0")
	}
	if config.ReplicaAddress == "" {
		return nil, fmt.Errorf("replica address is required")
	}
	if config.LeaderElectionTimeout == 0 {
		return nil, fmt.Errorf("leader election timeout is required")
	}
	if config.LeaderHeartbeatTimeout == 0 {
		return nil, fmt.Errorf("leader heartbeat timeout is required")
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
			nextLeaderElectionTimeout:  0,
			nextLeaderHeartbeatTimeout: 0,
		},
		logger: logger,
	}
	raft.logger = logger.With("replica", raft.ReplicaAddress())

	raft.logger.Debugf("startup, transitioning to follower")
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

func (raft *Raft) HandleUserRequest(typ uint8, value []byte) *types.UserRequestInput {
	request := &types.UserRequestInput{
		Type:   typ,
		Value:  value,
		DoneCh: make(chan error, 1),
	}

	raft.messageBus.QueueUserRequest(request)

	return request
}

func (raft *Raft) Tick() {
	raft.mutableState.currentTick++

	switch raft.mutableState.state {
	case Follower:
		if raft.leaderElectionTimeoutFired() {
			raft.logger.Debugf("tick=%d follower: election timeout fired", raft.mutableState.currentTick)
			raft.transitionToState(Candidate)
			return
		}
		if err := raft.handleMessages(); err != nil {
			raft.logger.Debugf("error handling messages: %s", err.Error())
		}
	case Candidate:
		if raft.leaderElectionTimeoutFired() {
			raft.logger.Debugf("tick=%d candidate: election timeout fired", raft.mutableState.currentTick)
			if err := raft.startElection(); err != nil {
				raft.logger.Debugf("error starting election: %s", err.Error())
			}
			raft.resetElectionTimeout()
		}
		if err := raft.handleMessages(); err != nil {
			raft.logger.Debugf("error handling messages: %s", err.Error())
		}
	case Leader:
		if raft.leaderHeartbeatTimeoutFired() {
			raft.logger.Debugf("tick=%d leader: heartbeat timeout fired", raft.mutableState.currentTick)
			// Send heartbeat with empty entries.
			raft.sendAppendEntries(make([]types.Entry, 0))
			raft.resetLeaderHeartbeatTimeout()
		}
		if err := raft.handleMessages(); err != nil {
			raft.logger.Debugf("error handling messages: %s", err.Error())
		}
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
	// TODO: jitter to avoid split votes (e.g., 150–300ms).
	nextTimeoutAtTick := raft.mutableState.nextLeaderElectionTimeout + uint64(raft.config.LeaderElectionTimeout.Milliseconds())

	raft.logger.Debugf("tick=%d nextTimeoutAtTick=%d resetting leader election timeout",
		raft.mutableState.currentTick,
		nextTimeoutAtTick,
	)

	raft.mutableState.nextLeaderElectionTimeout = nextTimeoutAtTick
}

func (raft *Raft) resetLeaderHeartbeatTimeout() {
	nextTimeoutAtTick := raft.mutableState.nextLeaderHeartbeatTimeout + uint64(raft.config.LeaderHeartbeatTimeout.Milliseconds())

	raft.logger.Debugf("tick=%d nextTimeoutAtTick=%d resetting heartbeat timeout", raft.mutableState.currentTick, nextTimeoutAtTick)

	raft.mutableState.nextLeaderHeartbeatTimeout = nextTimeoutAtTick
}

func (raft *Raft) startElection() error {
	votedFor := raft.mutableState.currentTermState.votedFor
	assert.True(votedFor == 0 || votedFor == raft.config.ReplicaID, "cannot have voted for someone and be in the candidate state")

	raft.logger.Debugf("starting election")

	raft.newTerm()

	if err := raft.voteFor(raft.config.ReplicaID, raft.mutableState.currentTermState.term); err != nil {
		return fmt.Errorf("candidate voting for itself: %w", err)
	}

	for _, replica := range raft.config.Replicas {
		raft.messageBus.RequestVote(raft.ReplicaAddress(), replica.ReplicaAddress, types.RequestVoteInput{
			CandidateTerm:         raft.mutableState.currentTermState.term,
			CandidateID:           raft.config.ReplicaID,
			CandidateLastLogIndex: 1,
			CandidateLastLogTerm:  1,
		})
	}

	return nil
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

	raft.logger.Debugf("term=%d changed to new term", raft.mutableState.currentTermState.term)

	return nil
}

func (raft *Raft) voteFor(candidateID types.ReplicaID, candidateTerm uint64) error {
	// TODO: persist vote aod not transition to follower
	// TODO: avoid 2 fsyncs
	raft.newTerm(withTerm(candidateTerm))

	if err := raft.storage.Persist(storage.State{CurrentTerm: candidateTerm, VotedFor: candidateID}); err != nil {
		return fmt.Errorf("persisting term and voted for: %w", err)
	}
	raft.mutableState.currentTermState.votedFor = candidateID

	raft.logger.Debugf("term=%d candidate=%d voted for candidate", candidateID, raft.mutableState.currentTermState.term)

	return nil
}

func (raft *Raft) State() State {
	return raft.mutableState.state
}

func (raft *Raft) transitionToState(state State) {
	raft.mutableState.state = state

	switch raft.mutableState.state {
	case Leader:
		raft.logger.Debugf("transitioning to leader")
		raft.resetLeaderHeartbeatTimeout()
	case Candidate:
		raft.logger.Debugf("transitioning to candidate")
	case Follower:
		raft.logger.Debugf("transitioning to follower")
		raft.resetElectionTimeout()
	default:
		panic(fmt.Sprintf("unexpected state: %d", raft.mutableState.state))
	}
}

func (raft *Raft) handleMessages() error {
	// TODO: ensure term is updated to max(message.term, replica.term)

	message, err := raft.messageBus.Receive(raft.ReplicaAddress())
	if message == nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error receiving message from message bus: %s", err)
	}

	switch message := message.(type) {
	case *types.UserRequestInput:
		message.DoneCh <- nil

	case *types.AppendEntriesInput:
		// TODO: replica becomes/stays follower if leader term is >= currentTerm

		raft.logger.Debugf("handling AppendEntries")

		replica := findReplicaByID(raft.config.Replicas, message.LeaderID)

		appendEntriesOutput := types.AppendEntriesOutput{
			CurrentTerm:      raft.mutableState.currentTermState.term,
			Success:          false,
			PreviousLogIndex: raft.storage.LastLogIndex(),
			PreviousLogTerm:  raft.storage.LastLogTerm(),
		}

		if message.LeaderTerm < raft.mutableState.currentTermState.term {
			raft.logger.Debugf("leader term is less than the curren term")
			raft.messageBus.SendAppendEntriesResponse(raft.config.ReplicaAddress, replica.ReplicaAddress, appendEntriesOutput)
			return nil
		}

		if message.PreviousLogIndex != raft.storage.LastLogIndex() {
			raft.logger.Debugf("message.PreviousLogIndex=%d lastLogIndex=%d log index is not the same as replicas last log index",
				message.PreviousLogIndex,
				raft.storage.LastLogIndex(),
			)
			raft.messageBus.SendAppendEntriesResponse(raft.config.ReplicaAddress, replica.ReplicaAddress, appendEntriesOutput)
			return nil
		}

		success := true

		if message.PreviousLogIndex > 0 {
			raft.logger.Debugf("message.PreviousLogIndex=%d getting entry at index", message.PreviousLogIndex)
			entry, err := raft.storage.GetEntryAtIndex(message.PreviousLogIndex)
			if err != nil && !errors.Is(err, storage.ErrIndexOutOfBounds) {
				return fmt.Errorf("getting entry at index: index=%d %w", message.PreviousLogIndex, err)
			}

			if entry != nil && entry.Term != message.PreviousLogTerm {
				raft.logger.Debugf("message.PreviousLogIndex=%d message.PreviousLogTerm=%d entry.Term=%d conflicting entries at log index",
					message.PreviousLogIndex,
					message.PreviousLogTerm,
					entry.Term,
				)
				success = false
				raft.logger.Debugf("previousLogIndex=%d truncating log", message.PreviousLogIndex)
				if err := raft.storage.TruncateLogStartingFrom(message.PreviousLogIndex); err != nil {
					return fmt.Errorf("truncating log: index=%d %w", message.PreviousLogIndex, err)
				}
			}
		}

		raft.logger.Debugf("len(entries)=%d appending entries", len(message.Entries))
		if err := raft.storage.AppendEntries(message.Entries); err != nil {
			return fmt.Errorf("appending entries to log: entries=%+v %w", message.Entries, err)
		}

		raft.resetElectionTimeout()

		if message.LeaderCommitIndex > raft.mutableState.commitIndex {
			raft.logger.Debugf("leaderCommitIndex=%d commitIndex=%d will apply uncommitted entries",
				message.LeaderCommitIndex,
				raft.mutableState.commitIndex,
			)
			if err := raft.applyUncommittedEntries(message.LeaderCommitIndex, raft.storage.LastLogIndex()); err != nil {
				return fmt.Errorf("applying uncomitted entries: leaderCommitIndex=%d lastLogIndex=%d %w", message.LeaderCommitIndex, raft.storage.LastLogIndex(), err)
			}
		}

		raft.mutableState.currentTermState.term = message.LeaderTerm

		raft.messageBus.SendAppendEntriesResponse(raft.config.ReplicaAddress, replica.ReplicaAddress, types.AppendEntriesOutput{
			CurrentTerm:      raft.mutableState.currentTermState.term,
			Success:          success,
			PreviousLogIndex: raft.storage.LastLogIndex(),
			PreviousLogTerm:  raft.storage.LastLogTerm(),
		})

	case *types.RequestVoteInput:
		raft.logger.Debugf("message=%+v handling RequestVote", message)

		replica := findReplicaByID(raft.config.Replicas, message.CandidateID)

		// Receiver implementation:
		// 1. Reply false if term < currentTerm (§5.1)
		// 2. If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		if message.CandidateTerm < raft.mutableState.currentTermState.term {
			raft.logger.Debugf("candidateTerm=%d replicaTerm=%d candidate term is less than replica term, not granting vote", message.CandidateTerm, raft.mutableState.currentTermState.term)
			raft.messageBus.SendRequestVoteResponse(raft.config.ReplicaAddress, replica.ReplicaAddress, types.RequestVoteOutput{
				CurrentTerm: raft.mutableState.currentTermState.term,
				VoteGranted: false,
			})
			return nil
		}

		alreadyVotedForCandidate := raft.hasVotedForCandidate(message.CandidateID)
		hasVoted := raft.hasVoted()
		candidateLogUpToDate := raft.isCandidateLogUpToDate(message)
		voteGranted := alreadyVotedForCandidate || (!hasVoted && candidateLogUpToDate)

		raft.logger.Debugf("voteGranted=%v alreadyVotedForCandidate=%v hasVoted=%v candidateLogUpToDate=%v",
			voteGranted,
			alreadyVotedForCandidate,
			hasVoted,
			candidateLogUpToDate,
		)

		if voteGranted {
			if err := raft.voteFor(message.CandidateID, message.CandidateTerm); err != nil {
				return fmt.Errorf("voting for candidate: candidateID=%d candidateTerm=%d %w err", message.CandidateID, message.CandidateTerm, err)
			}
			raft.transitionToState(Follower)
		}

		// TODO max(term)

		raft.messageBus.SendRequestVoteResponse(raft.ReplicaAddress(), replica.ReplicaAddress, types.RequestVoteOutput{
			CurrentTerm: raft.mutableState.currentTermState.term,
			VoteGranted: voteGranted,
		})

	case *types.RequestVoteOutput:
		raft.logger.Debugf("message=%+v handling RequestVoteOutput", message)

		if message.CurrentTerm > raft.mutableState.currentTermState.term {
			raft.newTerm(withTerm(message.CurrentTerm))
			raft.transitionToState(Follower)
			return nil
		}

		// TODO: test
		if message.VoteGranted && message.CurrentTerm == raft.mutableState.currentTermState.term {
			raft.mutableState.currentTermState.votesReceived++

			if raft.mutableState.currentTermState.votesReceived >= raft.majority() {
				raft.transitionToState(Leader)
				// Send empty heartbeat to avoid the other replicas election timeouts.
				raft.logger.Debugln("new leader is sending heartbeat after election")
				raft.sendAppendEntries(make([]types.Entry, 0))
			}
		}
	default:
		panic(fmt.Sprintf("unexpected message: %+v", message))
	}

	return nil
}

func (raft *Raft) sendAppendEntries(entries []types.Entry) {
	for _, replica := range raft.config.Replicas {
		raft.messageBus.SendAppendEntriesRequest(raft.ReplicaAddress(), replica.ReplicaAddress, types.AppendEntriesInput{
			LeaderID:          raft.config.ReplicaID,
			LeaderTerm:        raft.mutableState.currentTermState.term,
			LeaderCommitIndex: raft.mutableState.commitIndex,
			PreviousLogIndex:  raft.storage.LastLogIndex(),
			PreviousLogTerm:   raft.storage.LastLogTerm(),
			Entries:           entries,
		})
	}
}

func (raft *Raft) leaderElectionTimeoutFired() bool {
	return raft.mutableState.currentTick > raft.mutableState.nextLeaderElectionTimeout
}

func (raft *Raft) leaderHeartbeatTimeoutFired() bool {
	return raft.mutableState.currentTick > raft.mutableState.nextLeaderHeartbeatTimeout
}

func (raft *Raft) isCandidateLogUpToDate(input *types.RequestVoteInput) bool {
	upToDate := input.CandidateTerm >= raft.mutableState.currentTermState.term &&
		input.CandidateLastLogTerm >= raft.storage.LastLogTerm() &&
		input.CandidateLastLogIndex >= raft.storage.LastLogIndex()

	if !upToDate {
		raft.logger.Debugf("candidateID=%d candidateLastLogTerm=%d candidateLastLogIndex=%d candidate log is not up to date",
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
	raft.logger.Debugf("leaderCommitIndex=%d lastLogIndex=%d applying uncomitted entries",
		leaderCommitIndex,
		lastlogIndex,
	)
	commitIndex := leaderCommitIndex
	if lastlogIndex < leaderCommitIndex {
		commitIndex = lastlogIndex
	}

	for i := raft.mutableState.commitIndex; i <= commitIndex; i++ {
		if i == 0 {
			continue
		}

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
	panic(fmt.Sprintf("unreachable: unable to find replica with id. id=%d replicas=%+v", replicaID, replicas))
}
