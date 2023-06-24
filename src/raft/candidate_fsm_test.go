package raft

import (
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
)

func TestCandidateFSM(t *testing.T) {
	t.Parallel()

	t.Run("candidate transitions to follower when there's another replica with a term >= to its term", func(t *testing.T) {
		t.Parallel()

		t.Run("append entries request", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			candidate := cluster.Replicas[0]
			assert.NoError(t, candidate.transitionToState(Candidate))

			leader := cluster.Replicas[1]
			assert.NoError(t, leader.transitionToState(Leader))
			assert.NoError(t, leader.newTerm(withTerm(candidate.mutableState.currentTermState.term+1)))

			outgoingMessages, err := candidate.handleMessage(&types.AppendEntriesInput{
				LeaderID:          leader.Config.ReplicaID,
				LeaderTerm:        leader.mutableState.currentTermState.term,
				LeaderCommitIndex: 0,
				PreviousLogIndex:  0,
				PreviousLogTerm:   0,
				Entries:           make([]types.Entry, 0),
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(outgoingMessages))

			assert.True(t, outgoingMessages[0].Message.(*types.AppendEntriesOutput).Success)

			assert.Equal(t, Follower, candidate.State())
			assert.Equal(t, leader.mutableState.currentTermState.term, candidate.mutableState.currentTermState.term)
		})

		t.Run("request vote request", func(t *testing.T) {
			t.Parallel()

			cluster := Setup()

			candidateA := cluster.Replicas[0]
			assert.NoError(t, candidateA.transitionToState(Candidate))

			candidateB := cluster.Replicas[1]
			assert.NoError(t, candidateB.transitionToState(Candidate))

			assert.NoError(t, candidateA.newTerm(withTerm(candidateB.mutableState.currentTermState.term+1)))

			cluster.Bus.Send(candidateA.Config.ReplicaID, candidateB.Config.ReplicaID, &types.RequestVoteInput{
				MessageID:             1,
				CandidateID:           candidateA.Config.ReplicaID,
				CandidateTerm:         candidateA.mutableState.currentTermState.term,
				CandidateLastLogIndex: 0,
				CandidateLastLogTerm:  0,
			},
			)

			cluster.Bus.Tick()
			cluster.Network.Tick()

			candidateB.Tick()

			assert.Equal(t, Follower, candidateB.State())
			assert.Equal(t, candidateA.mutableState.currentTermState.term, candidateB.mutableState.currentTermState.term)
		})
	})

	t.Run("leader election timeout is reset when follower transitions to candidate", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Followers()[0]

		// Tick until the timeout fires.
		timeoutAtTick := replica.mutableState.nextLeaderElectionTimeout + 1
		for i := uint64(0); i < timeoutAtTick; i++ {
			replica.Tick()
		}
		// Tick one more time to enter the Candidate fsm.
		replica.Tick()

		assert.True(t, timeoutAtTick < replica.mutableState.nextLeaderElectionTimeout)
	})

	t.Run("candidate starts an election after transitioning from follower->candidate", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Followers()[0]

		termBeforeElection := replica.mutableState.currentTermState.term

		// Tick until the timeout fires.
		timeoutAtTick := replica.mutableState.nextLeaderElectionTimeout + 1
		for i := uint64(0); i < timeoutAtTick; i++ {
			replica.Tick()
		}
		// Tick one more time to enter the Candidate fsm.
		replica.Tick()

		t.Run("candidate starts a new term", func(t *testing.T) {
			assert.Equal(t, termBeforeElection+1, replica.mutableState.currentTermState.term)
		})

		t.Run("candidate votes for itself", func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, replica.Config.ReplicaID, replica.mutableState.currentTermState.votedFor)
			assert.Equal(t, map[types.ReplicaID]bool{replica.Config.ReplicaID: true}, replica.mutableState.currentTermState.votesReceived)
		})
	})

	t.Run("election timed out, candidate should restart election", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		replica := cluster.Followers()[0]

		t.Run("election timeout, follower->candidate, start election", func(t *testing.T) {
			termBeforeElection := replica.mutableState.currentTermState.term

			// Tick until the timeout fires.
			timeoutAtTick := replica.mutableState.nextLeaderElectionTimeout + 1 - replica.Clock.CurrentTick()
			for i := uint64(0); i < timeoutAtTick; i++ {
				replica.Tick()
			}
			// Tick one more time to enter the Candidate fsm.
			replica.Tick()

			// Term is incremented every new election.
			assert.Equal(t, termBeforeElection+1, replica.mutableState.currentTermState.term)
		})

		t.Run("current election timeout, starts new election", func(t *testing.T) {
			termBeforeElection := replica.mutableState.currentTermState.term

			// Tick until the timeout fires again.
			timeoutAtTick := replica.mutableState.nextLeaderElectionTimeout + 1 - replica.Clock.CurrentTick()
			for i := uint64(0); i < timeoutAtTick; i++ {
				replica.Tick()
			}

			// New election, new term.
			assert.Equal(t, termBeforeElection+1, replica.mutableState.currentTermState.term)
		})
	})

	t.Run("candidate becomes leader iff it receives the majority of votes for the same term", func(t *testing.T) {
		t.Parallel()

		cluster := Setup()

		candidate := cluster.Replicas[0]
		assert.NoError(t, candidate.transitionToState(Candidate))

		_, err := candidate.startElection()
		assert.NoError(t, err)

		// Candidate's log is empty.
		assert.Equal(t, uint64(0), candidate.Storage.LastLogIndex())

		replicaA := cluster.Replicas[1]

		// Responses from previous terms are not taken into account.
		_, err = candidate.handleMessage(&types.RequestVoteOutput{
			CurrentTerm: candidate.mutableState.currentTermState.term - 1,
			VoteGranted: true,
		})
		assert.NoError(t, err)

		_, err = candidate.handleMessage(&types.RequestVoteOutput{
			CurrentTerm: candidate.mutableState.currentTermState.term - 1,
			VoteGranted: true,
		})
		assert.NoError(t, err)

		assert.Equal(t, Candidate, candidate.State())

		// Candidate votes itself when an election is started.
		assert.Equal(t, uint16(1), candidate.votesReceived())

		// Duplicated messages are ignored.
		_, err = candidate.handleMessage(&types.RequestVoteOutput{
			ReplicaID:   replicaA.Config.ReplicaID,
			CurrentTerm: candidate.mutableState.currentTermState.term,
			VoteGranted: true,
		})
		assert.NoError(t, err)

		_, err = candidate.handleMessage(&types.RequestVoteOutput{
			ReplicaID:   replicaA.Config.ReplicaID,
			CurrentTerm: candidate.mutableState.currentTermState.term,
			VoteGranted: true,
		})
		assert.NoError(t, err)

		// Received two votes: itself and from replica A.
		assert.Equal(t, uint16(2), candidate.votesReceived())

		// Got majority votes, becomes leader.
		assert.Equal(t, Leader, candidate.State())

		// Process leader heartbeat.
		cluster.Tick()

		// Replica appends empty log entry upon becoming leader.
		assert.Equal(t, uint64(1), candidate.Storage.LastLogIndex())
		assert.Equal(t, candidate.mutableState.currentTermState.term, candidate.Storage.LastLogTerm())
	})
}
