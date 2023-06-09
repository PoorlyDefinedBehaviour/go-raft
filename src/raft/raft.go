package raft

import (
	"errors"
	"fmt"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"go.uber.org/zap"
)

const appendEntriesBatchSize = 100

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
	config Config

	// Used to send inputs to other replicas.
	messageBus *messagebus.MessageBus

	// The state mutated directly by the Raft struct.
	mutableState MutableState

	// The state machine being built on top of raft.
	stateMachine types.StateMachine

	// Long term storage.
	storage storage.Storage

	// Random number generator.
	rand rand.Random

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

	// The address of this replica.
	ReplicaAddress types.ReplicaAddress

	// Minimum amount of time to wait for without receiving a heartbeat from the leader to start an election.
	MinLeaderElectionTimeout time.Duration

	// Maximum amount of time to wait for without receiving a heartbeat from the leader to start an election.
	MaxLeaderElectionTimeout time.Duration

	// As a leader, how long to wait for before sending a heartbeat to replicas.
	LeaderHeartbeatTimeout time.Duration

	// The list of other replicas in the cluster. Should not include this replica.
	Replicas []Replica
}

type Replica struct {
	ReplicaID      types.ReplicaID
	ReplicaAddress types.ReplicaAddress
}

func NewRaft(config Config, messageBus *messagebus.MessageBus, storage storage.Storage, stateMachine types.StateMachine, rand rand.Random, logger *zap.SugaredLogger) (*Raft, error) {
	if config.ReplicaID == 0 {
		return nil, fmt.Errorf("replica id cannot be 0")
	}
	if config.ReplicaAddress == "" {
		return nil, fmt.Errorf("replica address is required")
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
				votesReceived: make(map[uint16]bool),
			},
			nextLeaderElectionTimeout:  0,
			nextLeaderHeartbeatTimeout: 0,
			nextIndex:                  newNextIndex(config.Replicas),
		},
		rand:   rand,
		logger: logger,
	}
	raft.logger = logger.With("replica", raft.ReplicaAddress())

	raft.transitionToState(Follower)
	raft.resetElectionTimeout()

	return raft, nil
}

func newNextIndex(replicas []Replica) map[types.ReplicaID]uint64 {
	out := make(map[types.ReplicaID]uint64)

	for _, replica := range replicas {
		out[replica.ReplicaID] = 1
	}

	return out
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

			if err := raft.sendHeartbeat(); err != nil {
				raft.logger.Error("sending heartbeat", "err", err)
			}
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
	nextTimeoutAtTick := raft.mutableState.nextLeaderElectionTimeout +
		raft.rand.GenBetween(
			uint64(raft.config.MinLeaderElectionTimeout.Milliseconds()),
			uint64(raft.config.MaxLeaderElectionTimeout.Milliseconds()),
		)

	raft.logger.Debugf("tick=%d nextTimeoutAtTick=%d new leader election timeout",
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
	raft.logger.Debugf("starting election")

	raft.newTerm()

	votedFor := raft.mutableState.currentTermState.votedFor
	assert.True(votedFor == 0 || votedFor == raft.config.ReplicaID, "cannot have voted for someone and be in the candidate state")

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
		votesReceived: make(map[uint16]bool),
	}
	for _, option := range options {
		option(&newState)
	}

	if raft.mutableState.currentTermState.term == newState.term {
		return nil
	}

	if err := raft.storage.Persist(storage.State{CurrentTerm: newState.term, VotedFor: newState.votedFor}); err != nil {
		return fmt.Errorf("persisting term and voted for: %w", err)
	}

	raft.mutableState.currentTermState = newState

	raft.logger.Debugf("term=%d changed to new term", raft.mutableState.currentTermState.term)

	return nil
}

func (raft *Raft) voteFor(candidateID types.ReplicaID, candidateTerm uint64) error {
	assert.True(raft.mutableState.currentTermState.votedFor == 0, fmt.Sprintf("votedFor=%d cannot vote again after having voted", raft.mutableState.currentTermState.votedFor))

	// TODO: persist vote aod not transition to follower
	// TODO: avoid 2 fsyncs
	raft.newTerm(withTerm(candidateTerm))

	if err := raft.storage.Persist(storage.State{CurrentTerm: candidateTerm, VotedFor: candidateID}); err != nil {
		return fmt.Errorf("persisting term and voted for: %w", err)
	}

	raft.mutableState.currentTermState.votedFor = candidateID

	if candidateID != raft.config.ReplicaID {
		raft.logger.Debugf("term=%d candidate=%d voted for candidate", candidateID, raft.mutableState.currentTermState.term)
	} else {
		raft.logger.Debugf("term=%d candidate=%d voted for itself", candidateID, raft.mutableState.currentTermState.term)

		assert.True(raft.State() == Candidate, "must be a candidate to vote for itself")

		raft.mutableState.currentTermState.votesReceived[raft.config.ReplicaID] = true
	}

	return nil
}

func (raft *Raft) State() State {
	return raft.mutableState.state
}

func (raft *Raft) transitionToState(state State) {
	if raft.mutableState.state == state {
		return
	}

	raft.logger.Debugf("%s -> %s", raft.mutableState.state, state)

	raft.mutableState.state = state

	switch raft.mutableState.state {
	case Leader:
		raft.resetLeaderHeartbeatTimeout()
	case Candidate:
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

		if message.LeaderTerm >= raft.mutableState.currentTermState.term {
			raft.logger.Debugf("leaderTerm=%d leader term is up to date transitioning to follower", message.LeaderTerm)
			raft.newTerm(withTerm(message.LeaderTerm))
			raft.transitionToState(Follower)
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

		if message.CandidateTerm >= raft.mutableState.currentTermState.term {
			raft.logger.Debugf("candidateTerm=%d candidate term is up to date, transitioning to follower", message.CandidateTerm)
			raft.newTerm(withTerm(message.CandidateTerm))
			raft.transitionToState(Follower)
		}

		alreadyVotedForCandidate := raft.hasVotedForCandidate(message.CandidateID)
		hasVoted := raft.hasVoted()
		candidateLogUpToDate := raft.isCandidateLogUpToDate(message)
		voteGranted := alreadyVotedForCandidate || (!hasVoted && candidateLogUpToDate)

		raft.logger.Debugf("candidateID=%d voteGranted=%v alreadyVotedForCandidate=%v hasVoted=%v candidateLogUpToDate=%v votedFor=%d",
			message.CandidateID,
			voteGranted,
			alreadyVotedForCandidate,
			hasVoted,
			candidateLogUpToDate,
			raft.mutableState.currentTermState.votedFor,
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

		if raft.hasReceivedVote(message) {
			raft.mutableState.currentTermState.votesReceived[message.ReplicaID] = true

			if raft.votesReceived() >= raft.majority() {
				raft.transitionToState(Leader)
				// Send empty heartbeat to avoid the other replicas election timeouts.
				raft.logger.Debugln("new leader is sending heartbeat after election")
				if err := raft.sendHeartbeat(); err != nil {
					raft.logger.Error("sending heartbeat", "err", err)
				}
			}
		}
	default:
		panic(fmt.Sprintf("unexpected message: %+v", message))
	}

	return nil
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

	if nextIndex > raft.storage.LastLogIndex() {
		return make([]types.Entry, 0), nil
	}

	entries, err := raft.storage.GetBatch(nextIndex, appendEntriesBatchSize)
	if err != nil {
		return entries, fmt.Errorf("fetching replica batch from storage: nextIndex=%d, %w", nextIndex, err)
	}

	return entries, nil
}

func (raft *Raft) sendHeartbeat() error {
	raft.logger.Debug("sending leader hearbeat")

	for _, replica := range raft.config.Replicas {
		entries, err := raft.getNextBatchForReplica(replica.ReplicaID)
		if err != nil {
			return fmt.Errorf("fetching batch for replica: replicaID=%d %w", replica.ReplicaID, err)
		}

		raft.logger.Debugf("sending %d entries to replica %d", len(entries), replica.ReplicaID)

		raft.messageBus.SendAppendEntriesRequest(raft.ReplicaAddress(), replica.ReplicaAddress, types.AppendEntriesInput{
			LeaderID:          raft.config.ReplicaID,
			LeaderTerm:        raft.mutableState.currentTermState.term,
			LeaderCommitIndex: raft.mutableState.commitIndex,
			PreviousLogIndex:  raft.storage.LastLogIndex(),
			PreviousLogTerm:   raft.storage.LastLogTerm(),
			Entries:           entries,
		})

		// TODO: shouldn't advance next index if request does not succeed
		raft.mutableState.nextIndex[replica.ReplicaID] += uint64(len(entries))
	}

	return nil
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
