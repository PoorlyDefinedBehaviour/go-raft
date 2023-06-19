package raft

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/poorlydefinedbehaviour/raft-go/src/assert"
	"github.com/poorlydefinedbehaviour/raft-go/src/clock"
	"github.com/poorlydefinedbehaviour/raft-go/src/constants"
	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
	"github.com/poorlydefinedbehaviour/raft-go/src/storage"
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

	// The Raft state mutated directly by the Raft struct.
	mutableState MutableState

	// The state machine being built on top of raft.
	stateMachine types.StateMachine

	// Long term Storage.
	Storage storage.Storage

	// Random number generator.
	rand rand.Random

	// Clock used for time operations.
	Clock clock.Clock

	// Represents the current request that's being sent to every replica.
	inFlightRequest *request
}

type request struct {
	successes map[types.ReplicaID]bool
	failures  map[types.ReplicaID]bool
	doneCh    chan error
}

type MutableState struct {
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

func NewRaft(
	config Config,
	messageBus *messagebus.MessageBus,
	storage storage.Storage,
	stateMachine types.StateMachine,
	rand rand.Random,
	clock clock.Clock,
) (*Raft, error) {
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
		Config:       config,
		bus:          messageBus,
		Storage:      storage,
		stateMachine: stateMachine,
		mutableState: MutableState{
			state: Follower,
			currentTermState: CurrentTermState{
				term:          0,
				votedFor:      0,
				votesReceived: make(map[uint16]bool),
			},
			nextLeaderElectionTimeout:  0,
			nextLeaderHeartbeatTimeout: 0,
			nextIndex:                  newNextIndex(config.Replicas),
		},
		rand:            rand,
		inFlightRequest: nil,
		Clock:           clock,
	}

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

	message = fmt.Sprintf("%s(%s) T=%d TICK=%d %s\n",
		stateLetter(raft.State()),
		raft.ReplicaAddress(),
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

	message = fmt.Sprintf("%s(%s) T=%d %s\n",
		stateLetter(raft.State()),
		raft.ReplicaAddress(),
		raft.mutableState.currentTermState.term,
		message,
	)

	fmt.Println(message)
}

func (raft *Raft) ReplicaAddress() types.ReplicaAddress {
	return raft.Config.ReplicaAddress
}

func (raft *Raft) Start() {
	for {
		raft.Tick()
		time.Sleep(1 * time.Millisecond)
	}
}

func newInFlightRequest(replicaID types.ReplicaID, doneCh chan error) *request {
	return &request{
		successes: map[types.ReplicaID]bool{
			// The replica itself succeeded in processing the request.
			replicaID: true,
		},
		failures: make(map[types.ReplicaID]bool),
		doneCh:   doneCh,
	}
}

func (raft *Raft) HandleUserRequest(ctx context.Context, typ uint8, value []byte) (chan error, error) {
	if raft.State() != Leader {
		return nil, fmt.Errorf("only leaders can handle requests")
	}

	if typ == types.HeartbeatEntryType {
		return nil, fmt.Errorf("request type is reserved. type=%d", typ)
	}

	// userRequest := &types.UserRequestInput{
	// 	Type:   typ,
	// 	Value:  value,
	// 	DoneCh: make(chan error, 1),
	// }

	panic("todo")
}

func (raft *Raft) Tick() {
	raft.Clock.Tick()

	switch raft.mutableState.state {
	case Follower:
		if raft.leaderElectionTimeoutFired() {
			raft.debug("tick=%d follower: election timeout fired", raft.Clock.CurrentTick())
			if err := raft.transitionToState(Candidate); err != nil {
				raft.error("transitioning to candidate: %s", err.Error())
			}
			return
		}

	case Candidate:
		if raft.leaderElectionTimeoutFired() {
			raft.debug("tick=%d candidate: election timeout fired", raft.Clock.CurrentTick())
			if err := raft.startElection(); err != nil {
				raft.debug("error starting election: %s", err.Error())
			}
			raft.resetElectionTimeout()
		}

	case Leader:
		if raft.leaderHeartbeatTimeoutFired() {

			raft.debug("HEARTBEAT TIMEOUT FIRED")
			if err := raft.sendHeartbeat(); err != nil {
				raft.error("sending heartbeat: %s", err)
			}
		}

	default:
		panic(fmt.Sprintf("unexpected raft state: %d", raft.mutableState.state))
	}

}

func (raft *Raft) majority() uint16 {
	return uint16(len(raft.Config.Replicas)/2 + 1)
}

func (raft *Raft) VotedForCandidateInCurrentTerm(candidateID uint16) bool {
	return raft.mutableState.currentTermState.votedFor == candidateID
}

func (raft *Raft) resetElectionTimeout() {
	nextTimeoutAtTick := raft.mutableState.nextLeaderElectionTimeout +
		raft.rand.GenBetween(
			uint64(raft.Config.MinLeaderElectionTimeout.Milliseconds()),
			uint64(raft.Config.MaxLeaderElectionTimeout.Milliseconds()),
		)

	raft.debug("tick=%d nextTimeoutAtTick=%d new leader election timeout",
		raft.Clock.CurrentTick(),
		nextTimeoutAtTick,
	)

	raft.mutableState.nextLeaderElectionTimeout = nextTimeoutAtTick
}

func (raft *Raft) resetLeaderHeartbeatTimeout() {
	nextTimeoutAtTick := raft.mutableState.nextLeaderHeartbeatTimeout + uint64(raft.Config.LeaderHeartbeatTimeout.Milliseconds())

	raft.debug("tick=%d TIMEOUT_AT=%d reset heartbeat timeout", raft.Clock.CurrentTick(), nextTimeoutAtTick)

	raft.mutableState.nextLeaderHeartbeatTimeout = nextTimeoutAtTick
}

func (raft *Raft) startElection() error {
	raft.debug("NEW_ELECTION")

	if err := raft.newTerm(); err != nil {
		return fmt.Errorf("starting election: starting new term: %w", err)
	}

	votedFor := raft.mutableState.currentTermState.votedFor
	assert.True(votedFor == 0 || votedFor == raft.Config.ReplicaID, "cannot have voted for someone and be in the candidate state")

	if err := raft.voteFor(raft.Config.ReplicaID, raft.mutableState.currentTermState.term); err != nil {
		return fmt.Errorf("candidate voting for itself: %w", err)
	}

	for _, replica := range raft.Config.Replicas {
		raft.debug("REQUEST VOTE REPLICA=%d", replica.ReplicaID)
		raft.bus.Send(raft.ReplicaAddress(), replica.ReplicaAddress, &types.RequestVoteInput{
			CandidateTerm:         raft.mutableState.currentTermState.term,
			CandidateID:           raft.Config.ReplicaID,
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
	// TODO: do we need to store state in the stabl storage? probably not
	if raft.mutableState.state == state {
		return nil
	}

	raft.debug("%s -> %s", raft.mutableState.state, state)

	switch state {
	case Leader:
		raft.debug("became leader, reset heartbeat timeout")
		raft.resetLeaderHeartbeatTimeout()

		raft.debug("append empty entry to log after becoming leader")
		if err := raft.Storage.AppendEntries([]types.Entry{{Term: raft.mutableState.currentTermState.term}}); err != nil {
			return fmt.Errorf("append empty entry after becoming leader: %w", err)
		}
	case Candidate:
	case Follower:
		raft.debug("transitioning to follower")
		raft.resetElectionTimeout()
	default:
		panic(fmt.Sprintf("unexpected state: %d", raft.mutableState.state))
	}

	raft.mutableState.state = state

	return nil
}

func (raft *Raft) OnMessage(from types.ReplicaAddress, message types.Message) {
	outgoingMessage, err := raft.handleMessage(message)
	if err != nil {
		raft.error("handling message: %s", err)
		return
	}
	if outgoingMessage != nil {
		raft.bus.Send(raft.ReplicaAddress(), from, outgoingMessage)
	}
}

func (raft *Raft) handleMessage(message types.Message) (types.Message, error) {
	// TODO: ensure term is updated to max(message.term, replica.term)

	switch message := message.(type) {
	case *types.UserRequestInput:
		assert.True(raft.State() == Leader, "must be leader to handle user request")

		raft.debug("user request in flight")
		raft.inFlightRequest = newInFlightRequest(raft.Config.ReplicaID, message.DoneCh)

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
		if err := raft.sendHeartbeat(); err != nil {
			return nil, fmt.Errorf("sending heartbeat: %w", err)
		}

	case *types.AppendEntriesInput:
		raft.debug("AppendEntriesInput REPLICA=%d TERM=%d COMMIT_INDEX=%d PREVIOUS_LOG_INDEX=%d PREVIOUS_LOG_TERM=%d ENTRIES=%d",
			message.LeaderID,
			message.LeaderTerm,
			message.LeaderCommitIndex,
			message.PreviousLogIndex,
			message.PreviousLogTerm,
			len(message.Entries),
		)

		replica := findReplicaByID(raft.Config.Replicas, message.LeaderID)

		appendEntriesOutput := types.AppendEntriesOutput{
			ReplicaID:        raft.Config.ReplicaID,
			CurrentTerm:      raft.mutableState.currentTermState.term,
			Success:          false,
			PreviousLogIndex: raft.Storage.LastLogIndex(),
			PreviousLogTerm:  raft.Storage.LastLogTerm(),
		}

		if message.LeaderTerm < raft.mutableState.currentTermState.term {
			raft.debug("LEADER_TERM=%d leader term is less than the current term", message.LeaderTerm)
			return &appendEntriesOutput, nil
		}

		if message.LeaderTerm >= raft.mutableState.currentTermState.term {
			raft.debug("AppendEntriesInput LEADER WITH HIGHER TERM")
			if err := raft.transitionToState(Follower); err != nil {
				return nil, fmt.Errorf("transitioning to follower after finding a replica with a higher term: %w", err)
			}
			if err := raft.newTerm(withTerm(message.LeaderTerm)); err != nil {
				return nil, fmt.Errorf("transitioning to new term after finding replica with a higher term: %w", err)
			}
		}

		if message.PreviousLogIndex != raft.Storage.LastLogIndex() {
			raft.debug("AppendEntriesInput message.PreviousLogIndex=%d lastLogIndex=%d log index is not the same as replicas last log index",
				message.PreviousLogIndex,
				raft.Storage.LastLogIndex(),
			)
			return &appendEntriesOutput, nil
		}

		if message.LeaderTerm >= raft.mutableState.currentTermState.term {
			raft.debug("AppendEntriesInput leader is up to date")
			if err := raft.newTerm(withTerm(message.LeaderTerm)); err != nil {
				return nil, fmt.Errorf("starting new term: %w", err)
			}
			if err := raft.transitionToState(Follower); err != nil {
				return nil, fmt.Errorf("transitioning to follower: %w", err)
			}
		}

		if message.PreviousLogIndex > 0 {
			raft.debug("AppendEntriesInputmessage.PreviousLogIndex=%d getting entry at index", message.PreviousLogIndex)
			entry, err := raft.Storage.GetEntryAtIndex(message.PreviousLogIndex)
			if err != nil && !errors.Is(err, storage.ErrIndexOutOfBounds) {
				return nil, fmt.Errorf("getting entry at index: index=%d %w", message.PreviousLogIndex, err)
			}

			if entry != nil && entry.Term != message.PreviousLogTerm {
				raft.debug("AppendEntriesInput message.PreviousLogIndex=%d message.PreviousLogTerm=%d entry.Term=%d conflicting entries at log index",
					message.PreviousLogIndex,
					message.PreviousLogTerm,
					entry.Term,
				)

				raft.debug("AppendEntriesInput previousLogIndex=%d truncating log", message.PreviousLogIndex)
				if err := raft.Storage.TruncateLogStartingFrom(message.PreviousLogIndex); err != nil {
					return nil, fmt.Errorf("truncating log: index=%d %w", message.PreviousLogIndex, err)
				}
			}
		}

		raft.debug("AppendEntriesInput append entries to log")
		if err := raft.Storage.AppendEntries(message.Entries); err != nil {
			return nil, fmt.Errorf("appending entries to log: entries=%+v %w", message.Entries, err)
		}

		raft.resetElectionTimeout()

		if message.LeaderCommitIndex > raft.mutableState.commitIndex {
			raft.debug("AppendEntriesInput leaderCommitIndex=%d commitIndex=%d will apply uncommitted entries",
				message.LeaderCommitIndex,
				raft.mutableState.commitIndex,
			)
			if err := raft.applyCommittedEntries(message.LeaderCommitIndex, raft.Storage.LastLogIndex()); err != nil {
				return nil, fmt.Errorf("applying uncomitted entries: leaderCommitIndex=%d lastLogIndex=%d %w", message.LeaderCommitIndex, raft.Storage.LastLogIndex(), err)
			}
		}

		raft.mutableState.currentTermState.term = message.LeaderTerm

		raft.bus.Send(raft.Config.ReplicaAddress, replica.ReplicaAddress, &types.AppendEntriesOutput{
			ReplicaID:        raft.Config.ReplicaID,
			CurrentTerm:      raft.mutableState.currentTermState.term,
			Success:          true,
			PreviousLogIndex: raft.Storage.LastLogIndex(),
			PreviousLogTerm:  raft.Storage.LastLogTerm(),
		})

		appendEntriesOutput.Success = true
		return &appendEntriesOutput, nil

	case *types.AppendEntriesOutput:
		raft.debug("AppendEntriesOutput TERM=%d REPLICA=%d SUCCESS=%t PREVIOUS_LOG_INDEX=%d PREVIOUS_LOG_TERM=%d",
			message.CurrentTerm,
			message.ReplicaID,
			message.Success,
			message.PreviousLogIndex,
			message.PreviousLogTerm,
		)

		assert.True(raft.inFlightRequest != nil, "received response but there's no in flight request")

		if message.CurrentTerm != raft.mutableState.currentTermState.term {
			raft.debug("AppendEntriesOutput STALE TERM=%d", message.CurrentTerm)
			break
		}

		if message.Success {
			raft.inFlightRequest.successes[message.ReplicaID] = true
		} else {
			raft.inFlightRequest.failures[message.ReplicaID] = true
		}

		if len(raft.inFlightRequest.successes) == int(raft.majority()) {
			if raft.inFlightRequest.doneCh != nil {
				raft.debug("handleMessages: send nil to done channel")
				raft.inFlightRequest.doneCh <- nil
				close(raft.inFlightRequest.doneCh)
			}
		} else if len(raft.inFlightRequest.failures) == int(raft.majority()) {
			if raft.inFlightRequest.doneCh != nil {
				raft.debug("handleMessages: send ErrNoQuorum to done channel")
				raft.inFlightRequest.doneCh <- ErrNoQuorum
				close(raft.inFlightRequest.doneCh)
			}
		}

	case *types.RequestVoteInput:
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
			return &types.RequestVoteOutput{
				CurrentTerm: raft.mutableState.currentTermState.term,
				VoteGranted: false,
			}, nil
		}

		if message.CandidateTerm >= raft.mutableState.currentTermState.term {
			raft.debug("RequestVoteInput candidate is up to date")
			if err := raft.newTerm(withTerm(message.CandidateTerm)); err != nil {
				return nil, fmt.Errorf("starting new term: %w", err)
			}
			if err := raft.transitionToState(Follower); err != nil {
				return nil, fmt.Errorf("transitioning to follower: %w", err)
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
				return nil, fmt.Errorf("voting for candidate: candidateID=%d candidateTerm=%d %w err", message.CandidateID, message.CandidateTerm, err)
			}
			if err := raft.transitionToState(Follower); err != nil {
				return nil, fmt.Errorf("transitioning to follower: %w", err)
			}
		}

		// TODO max(term)

		return &types.RequestVoteOutput{
			CurrentTerm: raft.mutableState.currentTermState.term,
			VoteGranted: voteGranted,
		}, nil

	case *types.RequestVoteOutput:
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

			if raft.votesReceived() >= raft.majority() {
				if err := raft.transitionToState(Leader); err != nil {
					return nil, fmt.Errorf("transitioning to leader: %w", err)
				}
				// Send empty heartbeat to avoid the other replicas election timeouts.
				raft.debug("NEW LEADER. SENDING HEARTBEAT")
				if err := raft.sendHeartbeat(); err != nil {
					raft.error("sending heartbeat: %s", err)
				}
			}
		}
	default:
		panic(fmt.Sprintf("unexpected message: %+v", message))
	}

	return nil, nil
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

func (raft *Raft) sendHeartbeat() error {
	raft.debug("heartbeat in flight")
	raft.inFlightRequest = newInFlightRequest(raft.Config.ReplicaID, nil)

	for _, replica := range raft.Config.Replicas {
		entries, err := raft.getNextBatchForReplica(replica.ReplicaID)
		if err != nil {
			return fmt.Errorf("fetching batch for replica: replicaID=%d %w", replica.ReplicaID, err)
		}

		previousLogIndex := raft.mutableState.nextIndex[replica.ReplicaID]
		if previousLogIndex > 0 {
			previousLogIndex--
		}
		previousLogTerm := 0
		if previousLogIndex > 0 {
			entry, err := raft.Storage.GetEntryAtIndex(previousLogIndex)
			if err != nil {
				return fmt.Errorf("getting entry at index: index=%d %w", previousLogIndex, err)
			}
			previousLogTerm = int(entry.Term)
		}

		raft.debug("REPLICA=%d ENTRIES=%d PREVIOUS_LOG_INDEX=%d PREVIOUS_LOG_TERM=%d", replica.ReplicaID, len(entries), previousLogIndex, previousLogTerm)

		raft.bus.Send(raft.ReplicaAddress(), replica.ReplicaAddress, &types.AppendEntriesInput{
			LeaderID:          raft.Config.ReplicaID,
			LeaderTerm:        raft.mutableState.currentTermState.term,
			LeaderCommitIndex: raft.mutableState.commitIndex,
			PreviousLogIndex:  previousLogIndex,
			PreviousLogTerm:   uint64(previousLogTerm),
			Entries:           entries,
		})

		// TODO: shouldn't advance next index if request does not succeed
		raft.mutableState.nextIndex[replica.ReplicaID] += uint64(len(entries))
	}

	// minIndexReplicatedInMajority, _ := mapx.MinValue(raft.mutableState.nextIndex)

	// if *minIndexReplicatedInMajority > raft.mutableState.commitIndex {
	// 	if err := raft.applyCommittedEntries(*minIndexReplicatedInMajority, raft.storage.LastLogIndex()); err != nil {
	// 		return fmt.Errorf("applying committed entries: %w", err)
	// 	}
	// 	raft.mutableState.commitIndex = *minIndexReplicatedInMajority
	// }

	raft.debug("sent heartbeat, reset heartbeat timeout")
	raft.resetLeaderHeartbeatTimeout()

	return nil
}

func (raft *Raft) leaderElectionTimeoutFired() bool {
	return raft.Clock.CurrentTick() >= raft.mutableState.nextLeaderElectionTimeout
}

func (raft *Raft) leaderHeartbeatTimeoutFired() bool {
	return raft.Clock.CurrentTick() >= raft.mutableState.nextLeaderHeartbeatTimeout
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

func findReplicaByID(replicas []Replica, replicaID types.ReplicaID) Replica {
	for _, replica := range replicas {
		if replica.ReplicaID == replicaID {
			return replica
		}
	}
	panic(fmt.Sprintf("unreachable: unable to find replica with id. id=%d replicas=%+v", replicaID, replicas))
}
