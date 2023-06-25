package raft

import (
	"errors"
	"fmt"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/clock"
	"github.com/poorlydefinedbehaviour/raft-go/src/constants"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/set"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	"github.com/poorlydefinedbehaviour/raft-go/src/timeout"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

const appendEntriesBatchSize = 100

var ErrNoQuorum = errors.New("did not get successful response from majority")

type State struct{ value uint8 }

func (state State) String() string {
	switch state {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	panic(fmt.Sprintf("unreachable: unknown state: %d", state))
}

var (
	Follower  State = State{value: 1}
	Candidate State = State{value: 2}
	Leader    State = State{value: 3}
)

type Raft struct {
	// The replica configuration.
	Config Config

	// Used to send inputs to other replicas.
	bus *messagebus.MessageBus

	// The state machine being built on top of raft.
	stateMachine types.StateMachine

	// Long term Storage.
	Storage storage.Storage

	// Random number generator.
	rand rand.Random

	// Clock used for time operations.
	Clock clock.Clock

	// The Raft state mutated directly by the Raft struct.
	mutableState MutableState

	heartbeatTimeout timeout.T

	leaderElectionTimeout timeout.T

	requests *set.T[*request]

	nextRequestID uint64
}

type request struct {
	requestID     uint64
	successes     *set.T[types.ReplicaID]
	failures      *set.T[types.ReplicaID]
	timeoutAtTick uint64
	// doneCh        chan error
}

type MutableState struct {
	// State that is reset every new term.
	currentTermState CurrentTermState

	// Index that has been committed by the leader.
	commitIndex uint64

	// The current state of this replica.
	state State

	nextIndex map[types.ReplicaID]uint64
}

type CurrentTermState struct {
	// The current term of this replica.
	term uint64

	// The candidate this replica voted for in the current term.
	votedFor uint16

	// The number of votes received in the current election if there's one.
	votesReceived map[types.ReplicaID]bool
}

type Config struct {
	// The ID of this replica.
	ReplicaID types.ReplicaID

	// Minimum amount of time to wait for without receiving a heartbeat from the leader to start an election.
	MinLeaderElectionTimeout time.Duration

	// Maximum amount of time to wait for without receiving a heartbeat from the leader to start an election.
	MaxLeaderElectionTimeout time.Duration

	// As a leader, how long to wait for before sending a heartbeat to replicas.
	LeaderHeartbeatTimeout time.Duration

	// The list of other replicas in the cluster. Includes this replica.
	Replicas []types.ReplicaID

	MaxInFlightRequests uint16
}

type OutgoingMessage struct {
	Message types.Message
	To      types.ReplicaID
}

func New(
	config Config,
	bus *messagebus.MessageBus,
	storage storage.Storage,
	stateMachine types.StateMachine,
	rand rand.Random,
	clock clock.Clock,
) (*Raft, error) {
	if config.ReplicaID == 0 {
		return nil, fmt.Errorf("replica id cannot be 0")
	}
	if config.MinLeaderElectionTimeout == 0 {
		return nil, fmt.Errorf("minimum leader election timeout is required")
	}
	if config.MaxLeaderElectionTimeout == 0 {
		return nil, fmt.Errorf("maximum leader election timeout is required")
	}
	if config.MaxLeaderElectionTimeout < config.MinLeaderElectionTimeout {
		return nil, fmt.Errorf("maximum leader election timeout must be greater than the minimum")
	}
	if config.LeaderHeartbeatTimeout == 0 {
		return nil, fmt.Errorf("leader heartbeat timeout is required")
	}
	if config.MaxInFlightRequests == 0 {
		return nil, fmt.Errorf("max in flight requests must be greater than 0")
	}

	raft := &Raft{
		Config:       config,
		bus:          bus,
		Storage:      storage,
		stateMachine: stateMachine,
		rand:         rand,
		mutableState: MutableState{
			state: Follower,
			currentTermState: CurrentTermState{
				term:          0,
				votedFor:      0,
				votesReceived: make(map[uint16]bool),
			},
			nextIndex: nil,
		},
		heartbeatTimeout: timeout.New(uint64(config.LeaderHeartbeatTimeout.Milliseconds())),
		leaderElectionTimeout: timeout.New(rand.GenBetween(
			uint64(config.MinLeaderElectionTimeout.Milliseconds()),
			uint64(config.MaxLeaderElectionTimeout.Milliseconds()),
		)),
		requests:      set.New[*request](),
		nextRequestID: 1,
		Clock:         clock,
	}

	bus.RegisterOnMessageCallback(raft.Config.ReplicaID, raft.OnMessage)

	stateBeforeStart, err := raft.Storage.GetState()
	if err != nil {
		return nil, fmt.Errorf("getting replica state from disk:%w", err)
	}
	if stateBeforeStart != nil {
		raft.mutableState.currentTermState.votedFor = stateBeforeStart.VotedFor
		raft.mutableState.currentTermState.term = stateBeforeStart.CurrentTerm
	}

	if err := raft.transitionToState(Follower); err != nil {
		return raft, fmt.Errorf("transitioning to follower: %w", err)
	}
	raft.leaderElectionTimeout.ResetAndFireAfter(raft.rand.GenBetween(
		uint64(raft.Config.MinLeaderElectionTimeout.Milliseconds()),
		uint64(raft.Config.MaxLeaderElectionTimeout.Milliseconds()),
	))

	return raft, nil
}

func newNextIndex(replicas []types.ReplicaID, nextIndex uint64) map[types.ReplicaID]uint64 {
	out := make(map[types.ReplicaID]uint64)

	for _, replica := range replicas {
		out[replica] = nextIndex
	}

	return out
}

func stateLetter(state State) string {
	switch state {
	case Leader:
		return "L"
	case Candidate:
		return "C"
	case Follower:
		return "F"
	default:
		panic(fmt.Sprintf("unknown state: %s", state))
	}
}

func (raft *Raft) debug(template string, args ...interface{}) {
	message := fmt.Sprintf(template, args...)

	message = fmt.Sprintf("%s(%d) T=%d TICK=%d %s\n",
		stateLetter(raft.State()),
		raft.Config.ReplicaID,
		raft.mutableState.currentTermState.term,
		raft.Clock.CurrentTick(),
		message,
	)

	if constants.Debug {
		fmt.Println(message)
	}
}

func (raft *Raft) error(template string, args ...interface{}) {
	message := fmt.Sprintf(template, args...)

	fmt.Printf("%s(%d) T=%d %s\n",
		stateLetter(raft.State()),
		raft.Config.ReplicaID,
		raft.mutableState.currentTermState.term,
		message,
	)
}

func (raft *Raft) newRequestID() uint64 {
	requestID := raft.nextRequestID

	raft.nextRequestID++

	return requestID
}

func (raft *Raft) Start() {
	raft.debug("Start()")
	for {
		raft.Tick()
		time.Sleep(1 * time.Millisecond)
	}
}

func newInFlightRequest(requestID uint64, doneCh chan error, timeout uint64) *request {
	return &request{
		requestID:     requestID,
		timeoutAtTick: timeout,
		successes:     set.New[types.ReplicaID](),
		failures:      set.New[types.ReplicaID](),
		// doneCh:        doneCh,
	}
}

func (raft *Raft) Tick() {
	raft.Clock.Tick()
	raft.heartbeatTimeout.Tick()
	raft.leaderElectionTimeout.Tick()

	raft.requests.Retain(func(request **request) bool {
		return (*request).timeoutAtTick > raft.Clock.CurrentTick()
	})

	if raft.heartbeatTimeout.Fired() {
		outgoingMessages, err := raft.onHeartbeatTimeout()
		if err != nil {
			raft.error("handling heartbeat timeout: %s", err)
			return
		}
		for _, message := range outgoingMessages {
			raft.bus.Send(raft.Config.ReplicaID, message.To, message.Message)
		}
	}
	if raft.leaderElectionTimeout.Fired() {
		outgoingMessages, err := raft.onLeaderElectionTimeout()
		if err != nil {
			raft.error("handling leader election timeout: %s", err)
			return
		}
		for _, message := range outgoingMessages {
			raft.bus.Send(raft.Config.ReplicaID, message.To, message.Message)
		}
	}
}

func (raft *Raft) onHeartbeatTimeout() ([]OutgoingMessage, error) {
	raft.debug("onHeartbeatTimeout()")
	raft.heartbeatTimeout.Reset()

	if raft.State() != Leader {
		return nil, nil
	}

	raft.debug("heartbeat timeout fired, sending heartbeat")
	outgoingMessages, err := raft.sendHeartbeat()
	if err != nil {
		return outgoingMessages, fmt.Errorf("generating heartbeat messages: %w", err)
	}

	return outgoingMessages, nil
}

func (raft *Raft) onLeaderElectionTimeout() ([]OutgoingMessage, error) {
	raft.leaderElectionTimeout.ResetAndFireAfter(raft.rand.GenBetween(
		uint64(raft.Config.MinLeaderElectionTimeout.Milliseconds()),
		uint64(raft.Config.MaxLeaderElectionTimeout.Milliseconds()),
	))

	if raft.State() == Leader {
		return nil, nil
	}

	if raft.State() == Follower {
		raft.debug("follower: leader did not send heartbeat, becoming candidate")
		if err := raft.transitionToState(Candidate); err != nil {
			return nil, fmt.Errorf("transitioning from follower to candidate: %w", err)
		}
	}

	raft.debug("candidate: election timed out, starting new election")
	outgoingMessages, err := raft.startElection()
	if err != nil {
		return outgoingMessages, fmt.Errorf("starting election: %w", err)
	}

	return outgoingMessages, nil
}

func (raft *Raft) majority() uint16 {
	return uint16(len(raft.Config.Replicas) / 2)
}

func (raft *Raft) VotedForCandidateInCurrentTerm(candidateID uint16) bool {
	return raft.mutableState.currentTermState.votedFor == candidateID
}

func (raft *Raft) startElection() ([]OutgoingMessage, error) {
	raft.debug("NEW_ELECTION")

	request := newInFlightRequest(raft.newRequestID(), nil, raft.Clock.CurrentTick()+100)

	if raft.requests.Size() >= int(raft.Config.MaxInFlightRequests) {
		return nil, fmt.Errorf("request queue is full")
	}
	raft.requests.Insert(request)

	if err := raft.newTerm(); err != nil {
		return nil, fmt.Errorf("starting election: starting new term: %w", err)
	}

	votedFor := raft.mutableState.currentTermState.votedFor
	assert.True(votedFor == 0 || votedFor == raft.Config.ReplicaID, "cannot have voted for someone and be in the candidate state")

	if err := raft.voteFor(raft.Config.ReplicaID, raft.mutableState.currentTermState.term); err != nil {
		return nil, fmt.Errorf("candidate voting for itself: %w", err)
	}

	messages := make([]OutgoingMessage, 0, len(raft.Config.Replicas)-1)

	for _, replica := range raft.Config.Replicas {
		if replica == raft.Config.ReplicaID {
			continue
		}

		raft.debug("REQUEST VOTE REPLICA=%d", replica)

		messages = append(messages, OutgoingMessage{
			To: replica,
			Message: &types.RequestVoteInput{
				MessageID:             request.requestID,
				CandidateTerm:         raft.mutableState.currentTermState.term,
				CandidateID:           raft.Config.ReplicaID,
				CandidateLastLogIndex: raft.Storage.LastLogIndex(),
				CandidateLastLogTerm:  raft.Storage.LastLogTerm(),
			},
		})
	}

	return messages, nil
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
		votesReceived: make(map[uint16]bool),
	}
	for _, option := range options {
		option(&newState)
	}

	if raft.mutableState.currentTermState.term == newState.term {
		return nil
	}

	if err := raft.Storage.Persist(storage.State{CurrentTerm: newState.term, VotedFor: newState.votedFor}); err != nil {
		return fmt.Errorf("persisting term and voted for: %w", err)
	}

	raft.mutableState.currentTermState = newState

	raft.debug("NEW TERM")

	return nil
}

func (raft *Raft) voteFor(candidateID types.ReplicaID, candidateTerm uint64) error {
	votedFor := raft.mutableState.currentTermState.votedFor
	assert.True(votedFor == 0 || votedFor == candidateID, fmt.Sprintf("votedFor=%d cannot vote again after having voted", raft.mutableState.currentTermState.votedFor))

	// TODO: avoid 2 fsyncs
	if err := raft.newTerm(withTerm(candidateTerm)); err != nil {
		return fmt.Errorf("starting new term: candidateTerm=%d %w", candidateID, err)
	}

	if err := raft.Storage.Persist(storage.State{CurrentTerm: candidateTerm, VotedFor: candidateID}); err != nil {
		return fmt.Errorf("persisting term and voted for: %w", err)
	}

	raft.mutableState.currentTermState.votedFor = candidateID

	if candidateID != raft.Config.ReplicaID {
		raft.debug("VOTE=%d", candidateID)
	} else {
		raft.debug("VOTE=self")

		assert.True(raft.State() == Candidate, "must be a candidate to vote for itself")

		raft.mutableState.currentTermState.votesReceived[raft.Config.ReplicaID] = true
	}

	return nil
}

func (raft *Raft) State() State {
	return raft.mutableState.state
}

func (raft *Raft) Term() uint64 {
	return raft.mutableState.currentTermState.term
}

func (raft *Raft) transitionToState(state State) error {
	// TODO: do we need to store state in the stable storage? probably not
	// because the replica starts as follower.
	if raft.mutableState.state == state {
		return nil
	}

	raft.debug("TRANSITION %s -> %s", raft.mutableState.state, state)

	switch state {
	case Leader:
		raft.debug("became leader, reset heartbeat timeout")
		raft.heartbeatTimeout.Reset()

		raft.debug("append empty entry to log after becoming leader")
		nextIndex := raft.Storage.LastLogIndex() + 1

		entry := types.Entry{
			Term: raft.mutableState.currentTermState.term,
			Type: types.NewLeaderEntryType,
		}

		if err := raft.Storage.AppendEntries([]types.Entry{entry}); err != nil {
			return fmt.Errorf("append empty entry after becoming leader: %w", err)
		}

		raft.mutableState.nextIndex = newNextIndex(raft.Config.Replicas, nextIndex)
	case Candidate:
	case Follower:
		raft.debug("transitioning to follower")
		raft.leaderElectionTimeout.ResetAndFireAfter(raft.rand.GenBetween(
			uint64(raft.Config.MinLeaderElectionTimeout.Milliseconds()),
			uint64(raft.Config.MaxLeaderElectionTimeout.Milliseconds()),
		))
	default:
		panic(fmt.Sprintf("unexpected state: %d", raft.mutableState.state))
	}

	raft.mutableState.state = state

	return nil
}

func (raft *Raft) OnMessage(from types.ReplicaID, message types.Message) {
	outgoingMessages, err := raft.handleMessage(message)
	if err != nil {
		raft.error("handling message: %s", err)
		return
	}

	for _, message := range outgoingMessages {
		raft.bus.Send(raft.Config.ReplicaID, message.To, message.Message)
	}
}

func (raft *Raft) handleMessage(message types.Message) ([]OutgoingMessage, error) {
	// TODO: ensure term is updated to max(message.term, replica.term)

	switch message := message.(type) {
	case *types.UserRequestInput:
		return raft.onUserRequestInput(message)
	case *types.AppendEntriesInput:
		outgoingMessage, err := raft.onAppendEntriesInput(message)
		if err != nil {
			return nil, fmt.Errorf("handling append entries input: %w", err)
		}
		return []OutgoingMessage{outgoingMessage}, nil
	case *types.AppendEntriesOutput:
		if err := raft.onAppendEntriesOutput(message); err != nil {
			return nil, fmt.Errorf("handling append entries output: %w", err)
		}
		return nil, nil
	case *types.RequestVoteInput:
		outgoingMessage, err := raft.onRequestVoteInput(message)
		if err != nil {
			return nil, fmt.Errorf("handling request vote input: %w", err)
		}
		return []OutgoingMessage{outgoingMessage}, nil
	case *types.RequestVoteOutput:
		return raft.onRequestVoteOutput(message)
	default:
		panic(fmt.Sprintf("unexpected message: %+v", message))
	}
}

func (raft *Raft) onUserRequestInput(message *types.UserRequestInput) ([]OutgoingMessage, error) {
	assert.True(raft.State() == Leader, "must be leader to handle user request")

	raft.debug("user request in received")

	raft.debug("leader: append user request entries to log")
	entries := []types.Entry{{
		Term:  raft.mutableState.currentTermState.term,
		Type:  message.Type,
		Value: message.Value,
	}}
	if err := raft.Storage.AppendEntries(entries); err != nil {
		return nil, fmt.Errorf("appending entries to log: entries=%+v %w", entries, err)
	}
	raft.debug("NEW ENTRIES. SENDING HEARTBEAT")
	outgoingMessages, err := raft.sendHeartbeat()
	if err != nil {
		return outgoingMessages, fmt.Errorf("handling user request input: sending new entries heartbeat: %w", err)
	}

	return outgoingMessages, nil
}

func (raft *Raft) onAppendEntriesInput(message *types.AppendEntriesInput) (OutgoingMessage, error) {
	raft.debug("AppendEntriesInput REPLICA=%d TERM=%d COMMIT_INDEX=%d PREVIOUS_LOG_INDEX=%d PREVIOUS_LOG_TERM=%d ENTRIES=%d",
		message.LeaderID,
		message.LeaderTerm,
		message.LeaderCommitIndex,
		message.PreviousLogIndex,
		message.PreviousLogTerm,
		len(message.Entries),
	)

	outgoingMessage := OutgoingMessage{
		To: message.LeaderID,
		Message: &types.AppendEntriesOutput{
			MessageID:        message.ID(),
			ReplicaID:        raft.Config.ReplicaID,
			CurrentTerm:      raft.mutableState.currentTermState.term,
			Success:          false,
			PreviousLogIndex: raft.Storage.LastLogIndex(),
			PreviousLogTerm:  raft.Storage.LastLogTerm(),
		},
	}

	if message.LeaderTerm < raft.mutableState.currentTermState.term {
		raft.debug("LEADER_TERM=%d leader term is less than the current term", message.LeaderTerm)
		return outgoingMessage, nil
	}

	if message.LeaderTerm >= raft.mutableState.currentTermState.term {
		raft.debug("AppendEntriesInput LEADER WITH HIGHER TERM")
		if err := raft.transitionToState(Follower); err != nil {
			return outgoingMessage, fmt.Errorf("transitioning to follower after finding a replica with a higher term: %w", err)
		}
		if err := raft.newTerm(withTerm(message.LeaderTerm)); err != nil {
			return outgoingMessage, fmt.Errorf("transitioning to new term after finding replica with a higher term: %w", err)
		}
	}

	if message.PreviousLogIndex != raft.Storage.LastLogIndex() {
		raft.debug("AppendEntriesInput message.PreviousLogIndex=%d lastLogIndex=%d log index is not the same as replicas last log index",
			message.PreviousLogIndex,
			raft.Storage.LastLogIndex(),
		)
		return outgoingMessage, nil
	}

	if message.LeaderTerm >= raft.mutableState.currentTermState.term {
		raft.debug("AppendEntriesInput leader is up to date")
		if err := raft.newTerm(withTerm(message.LeaderTerm)); err != nil {
			return outgoingMessage, fmt.Errorf("starting new term: %w", err)
		}
		if err := raft.transitionToState(Follower); err != nil {
			return outgoingMessage, fmt.Errorf("transitioning to follower: %w", err)
		}
	}

	if message.PreviousLogIndex > 0 {
		raft.debug("AppendEntriesInputmessage.PreviousLogIndex=%d getting entry at index", message.PreviousLogIndex)
		entry, err := raft.Storage.GetEntryAtIndex(message.PreviousLogIndex)
		if err != nil && !errors.Is(err, storage.ErrIndexOutOfBounds) {
			return outgoingMessage, fmt.Errorf("getting entry at index: index=%d %w", message.PreviousLogIndex, err)
		}

		if entry != nil && entry.Term != message.PreviousLogTerm {
			raft.debug("AppendEntriesInput message.PreviousLogIndex=%d message.PreviousLogTerm=%d entry.Term=%d conflicting entries at log index",
				message.PreviousLogIndex,
				message.PreviousLogTerm,
				entry.Term,
			)

			raft.debug("AppendEntriesInput previousLogIndex=%d truncating log", message.PreviousLogIndex)
			if err := raft.Storage.TruncateLogStartingFrom(message.PreviousLogIndex); err != nil {
				return outgoingMessage, fmt.Errorf("truncating log: index=%d %w", message.PreviousLogIndex, err)
			}
		}
	}

	raft.debug("AppendEntriesInput append entries to log")
	if err := raft.Storage.AppendEntries(message.Entries); err != nil {
		return outgoingMessage, fmt.Errorf("appending entries to log: entries=%+v %w", message.Entries, err)
	}

	raft.leaderElectionTimeout.ResetAndFireAfter(raft.rand.GenBetween(
		uint64(raft.Config.MinLeaderElectionTimeout.Milliseconds()),
		uint64(raft.Config.MaxLeaderElectionTimeout.Milliseconds()),
	))

	if message.LeaderCommitIndex > raft.mutableState.commitIndex {
		raft.debug("AppendEntriesInput leaderCommitIndex=%d commitIndex=%d will apply uncommitted entries",
			message.LeaderCommitIndex,
			raft.mutableState.commitIndex,
		)
		if err := raft.applyCommittedEntries(message.LeaderCommitIndex, raft.Storage.LastLogIndex()); err != nil {
			return outgoingMessage, fmt.Errorf("applying uncomitted entries: leaderCommitIndex=%d lastLogIndex=%d %w", message.LeaderCommitIndex, raft.Storage.LastLogIndex(), err)
		}
	}

	raft.mutableState.currentTermState.term = message.LeaderTerm

	return OutgoingMessage{
		To: message.LeaderID,
		Message: &types.AppendEntriesOutput{
			MessageID:        message.ID(),
			ReplicaID:        raft.Config.ReplicaID,
			CurrentTerm:      raft.mutableState.currentTermState.term,
			Success:          true,
			PreviousLogIndex: raft.Storage.LastLogIndex(),
			PreviousLogTerm:  raft.Storage.LastLogTerm(),
		},
	}, nil
}

func (raft *Raft) onAppendEntriesOutput(message *types.AppendEntriesOutput) error {
	raft.debug("AppendEntriesOutput TERM=%d REPLICA=%d SUCCESS=%t PREVIOUS_LOG_INDEX=%d PREVIOUS_LOG_TERM=%d",
		message.CurrentTerm,
		message.ReplicaID,
		message.Success,
		message.PreviousLogIndex,
		message.PreviousLogTerm,
	)

	if message.CurrentTerm != raft.mutableState.currentTermState.term {
		raft.debug("AppendEntriesOutput STALE TERM=%d", message.CurrentTerm)
		return nil
	}

	req, found := raft.requests.Find(func(request **request) bool { return (*request).requestID == message.MessageID })
	if !found {
		raft.debug("message not found in queue ID=%d REPLICA=%d", message.MessageID, message.ReplicaID)
		return nil
	}
	defer raft.requests.Remove(req)

	if req.successes.Contains(message.ReplicaID) || req.failures.Contains(message.ReplicaID) {
		raft.debug("duplicated message, ignore ID=%d REPLICA=%d", message.MessageID, message.ReplicaID)
		return nil
	}

	if message.Success {
		raft.debug("success=true append entries response ID=%d REPLICA=%d", message.MessageID, message.ReplicaID)
		req.successes.Insert(message.ReplicaID)
	} else {
		raft.debug("success=false append entries response ID=%d REPLICA=%d", message.MessageID, message.ReplicaID)
		req.failures.Insert(message.ReplicaID)
	}

	// var doneChErr error

	if req.successes.Size() == int(raft.majority())-1 {
		// doneChErr = nil
		raft.debug("handleMessages: send nil to done channel")
	} else if req.failures.Size() == int(raft.majority())-1 {
		raft.debug("handleMessages: send ErrNoQuorum to done channel")
		// doneChErr = ErrNoQuorum

	}

	// if req.doneCh != nil {
	// 	req.doneCh <- doneChErr
	// 	close(req.doneCh)
	// }

	return nil
}

func (raft *Raft) onRequestVoteInput(message *types.RequestVoteInput) (OutgoingMessage, error) {
	raft.debug("RequestVoteInput REPLICA_ID=%d REPLICA_TERM=%d LOG_INDEX=%d LOG_TERM=%d",
		message.CandidateID,
		message.CandidateTerm,
		message.CandidateLastLogIndex,
		message.CandidateLastLogTerm,
	)

	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if message.CandidateTerm < raft.mutableState.currentTermState.term {
		raft.debug("RequestVoteInput andidateTerm=%d replicaTerm=%d candidate term is less than replica term, not granting vote", message.CandidateTerm, raft.mutableState.currentTermState.term)
		return OutgoingMessage{
			To: message.CandidateID,
			Message: &types.RequestVoteOutput{
				MessageID:   message.ID(),
				CurrentTerm: raft.mutableState.currentTermState.term,
				VoteGranted: false,
			},
		}, nil
	}

	if message.CandidateTerm >= raft.mutableState.currentTermState.term {
		raft.debug("RequestVoteInput candidate is up to date")
		if err := raft.newTerm(withTerm(message.CandidateTerm)); err != nil {
			return OutgoingMessage{}, fmt.Errorf("starting new term: %w", err)
		}
		if err := raft.transitionToState(Follower); err != nil {
			return OutgoingMessage{}, fmt.Errorf("transitioning to follower: %w", err)
		}
	}

	alreadyVotedForCandidate := raft.hasVotedForCandidate(message.CandidateID)
	hasVoted := raft.hasVoted()
	candidateLogUpToDate := raft.isCandidateLogUpToDate(message)
	voteGranted := alreadyVotedForCandidate || (!hasVoted && candidateLogUpToDate)

	raft.debug("RequestVoteInput VOTE_GRANTED=%v VOTED_FOR_CANDIDATE=%v HAS_VOTED=%v LOG_UP_TO_DATE=%v VOTED_FOR=%d",
		voteGranted,
		alreadyVotedForCandidate,
		hasVoted,
		candidateLogUpToDate,
		raft.mutableState.currentTermState.votedFor,
	)

	if voteGranted {
		if err := raft.voteFor(message.CandidateID, message.CandidateTerm); err != nil {
			return OutgoingMessage{}, fmt.Errorf("voting for candidate: candidateID=%d candidateTerm=%d %w err", message.CandidateID, message.CandidateTerm, err)
		}
		if err := raft.transitionToState(Follower); err != nil {
			return OutgoingMessage{}, fmt.Errorf("transitioning to follower: %w", err)
		}
	}

	// TODO max(term)

	return OutgoingMessage{
		To: message.CandidateID,
		Message: &types.RequestVoteOutput{
			MessageID:   message.ID(),
			CurrentTerm: raft.mutableState.currentTermState.term,
			VoteGranted: voteGranted,
		},
	}, nil
}

func (raft *Raft) onRequestVoteOutput(message *types.RequestVoteOutput) ([]OutgoingMessage, error) {
	raft.debug("RequestVoteOutput REPLICA=%d REPLICA_TERM=%d VOTE_GRANTED=%t ", message.ReplicaID, message.CurrentTerm, message.VoteGranted)

	if message.CurrentTerm > raft.mutableState.currentTermState.term {
		if err := raft.newTerm(withTerm(message.CurrentTerm)); err != nil {
			return nil, fmt.Errorf("starting new term: %w", err)
		}
		if err := raft.transitionToState(Follower); err != nil {
			return nil, fmt.Errorf("transitioning to follower: %w", err)
		}
		return nil, nil
	}

	if raft.hasReceivedVote(message) {
		raft.mutableState.currentTermState.votesReceived[message.ReplicaID] = true

		if raft.votesReceived() >= raft.majority()-1 {
			outgoingMessages, err := raft.becomeLeader()
			if err != nil {
				return outgoingMessages, fmt.Errorf("becoming leader: %w", err)
			}
			return outgoingMessages, nil
		}
	}

	return nil, nil
}

func (raft *Raft) becomeLeader() ([]OutgoingMessage, error) {
	if err := raft.transitionToState(Leader); err != nil {
		return nil, fmt.Errorf("transitioning to leader: %w", err)
	}
	// Send empty heartbeat to avoid the other replicas election timeouts.
	raft.debug("NEW LEADER. SENDING HEARTBEAT")
	outgoingMessages, err := raft.sendHeartbeat()
	if err != nil {
		return outgoingMessages, fmt.Errorf("new leader: generating heartbeats: %w", err)
	}

	return outgoingMessages, nil
}

func (raft *Raft) votesReceived() uint16 {
	return uint16(len(raft.mutableState.currentTermState.votesReceived))
}

func (raft *Raft) hasReceivedVote(message *types.RequestVoteOutput) bool {
	return message.VoteGranted && message.CurrentTerm == raft.mutableState.currentTermState.term &&
		!raft.mutableState.currentTermState.votesReceived[message.ReplicaID]
}

func (raft *Raft) getNextBatchForReplica(replicaID types.ReplicaID) ([]types.Entry, error) {
	nextIndex := raft.mutableState.nextIndex[replicaID]

	if nextIndex > raft.Storage.LastLogIndex() {
		return make([]types.Entry, 0), nil
	}

	entries, err := raft.Storage.GetBatch(nextIndex, appendEntriesBatchSize)
	if err != nil {
		return entries, fmt.Errorf("fetching replica batch from storage: nextIndex=%d, %w", nextIndex, err)
	}

	return entries, nil
}

func (raft *Raft) sendHeartbeat() ([]OutgoingMessage, error) {
	raft.debug("heartbeat in flight")

	request := newInFlightRequest(raft.newRequestID(), nil, raft.Clock.CurrentTick()+100)

	if raft.requests.Size() >= int(raft.Config.MaxInFlightRequests) {
		return nil, fmt.Errorf("request queue is full")
	}
	raft.requests.Insert(request)

	messages := make([]OutgoingMessage, 0, len(raft.Config.Replicas)-1)

	for _, replica := range raft.Config.Replicas {
		if replica == raft.Config.ReplicaID {
			continue
		}

		entries, err := raft.getNextBatchForReplica(replica)
		if err != nil {
			return nil, fmt.Errorf("fetching batch for replica: replicaID=%d %w", replica, err)
		}

		var previousLogIndex uint64 = 0
		var previousLogTerm uint64 = 0

		nextIndex := raft.mutableState.nextIndex[replica]
		if nextIndex > 0 {
			previousLogIndex = nextIndex - 1
		}

		if previousLogIndex > 0 {
			previousEntry, err := raft.Storage.GetEntryAtIndex(previousLogIndex)
			if err != nil {
				return messages, fmt.Errorf("getting entry at index: index=%d :%w", previousLogIndex, err)
			}
			previousLogTerm = previousEntry.Term
		}

		raft.debug("REPLICA=%d ENTRIES=%d PREVIOUS_LOG_INDEX=%d PREVIOUS_LOG_TERM=%d", replica, len(entries), previousLogIndex, previousLogTerm)

		messages = append(messages, OutgoingMessage{
			To: replica,
			Message: &types.AppendEntriesInput{
				MessageID:         request.requestID,
				LeaderID:          raft.Config.ReplicaID,
				LeaderTerm:        raft.mutableState.currentTermState.term,
				LeaderCommitIndex: raft.mutableState.commitIndex,
				PreviousLogIndex:  previousLogIndex,
				PreviousLogTerm:   previousLogTerm,
				Entries:           entries,
			},
		})

		// raft.bus.Send(raft.Config.ReplicaID, replica.ReplicaAddress, input)

		// TODO: shouldn't advance next index if request does not succeed
		// raft.mutableState.nextIndex[replica] += uint64(len(entries))
	}

	// minIndexReplicatedInMajority, _ := mapx.MinValue(raft.mutableState.nextIndex)

	// if *minIndexReplicatedInMajority > raft.mutableState.commitIndex {
	// 	if err := raft.applyCommittedEntries(*minIndexReplicatedInMajority, raft.storage.LastLogIndex()); err != nil {
	// 		return fmt.Errorf("applying committed entries: %w", err)
	// 	}
	// 	raft.mutableState.commitIndex = *minIndexReplicatedInMajority
	// }

	raft.debug("sent heartbeat, reset heartbeat timeout")
	raft.heartbeatTimeout.Reset()

	return messages, nil
}

func (raft *Raft) isCandidateLogUpToDate(input *types.RequestVoteInput) bool {
	upToDate := input.CandidateTerm >= raft.mutableState.currentTermState.term &&
		input.CandidateLastLogTerm >= raft.Storage.LastLogTerm() &&
		input.CandidateLastLogIndex >= raft.Storage.LastLogIndex()

	if !upToDate {
		raft.debug("candidateID=%d candidateLastLogTerm=%d candidateLastLogIndex=%d candidate log is not up to date",
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

func (raft *Raft) applyCommittedEntries(leaderCommitIndex uint64, lastlogIndex uint64) error {
	raft.debug("leaderCommitIndex=%d lastLogIndex=%d applying uncomitted entries",
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

		entry, err := raft.Storage.GetEntryAtIndex(i)
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
