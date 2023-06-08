package raft

import (
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
	"github.com/stretchr/testify/assert"
)

func TestHandleMessagesRequestVote(t *testing.T) {
	t.Parallel()

	t.Run("candidate's term is less than the replica's term, should not grant vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		assert.NoError(t, replica.newTerm(withTerm(1)))

		env.Bus.RequestVote(candidate.ReplicaAddress(), replica.ReplicaAddress(), types.RequestVoteInput{
			CandidateID:           candidate.config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})

		env.Network.Tick()

		replica.handleMessages()

		env.Network.Tick()

		message, err := env.Bus.Receive(candidate.ReplicaAddress())
		assert.NoError(t, err)

		response := message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate's log is smaller than the replica's log, should not grante vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		replica.storage.AppendEntries([]types.Entry{{Term: 0}})

		env.Bus.RequestVote(candidate.ReplicaAddress(), replica.ReplicaAddress(), types.RequestVoteInput{
			CandidateID:           candidate.config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})

		env.Network.Tick()

		replica.handleMessages()

		env.Network.Tick()

		message, err := env.Bus.Receive(candidate.ReplicaAddress())
		assert.NoError(t, err)

		response := message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate last log term is smaller than the replica's last log term, should not grant vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		replica.storage.AppendEntries([]types.Entry{{Term: 1}})

		env.Bus.RequestVote(candidate.ReplicaAddress(), replica.ReplicaAddress(), types.RequestVoteInput{
			CandidateID:           candidate.config.ReplicaID,
			CandidateTerm:         1,
			CandidateLastLogIndex: 1,
			CandidateLastLogTerm:  0,
		})

		env.Network.Tick()

		replica.handleMessages()

		env.Network.Tick()

		message, err := env.Bus.Receive(candidate.ReplicaAddress())
		assert.NoError(t, err)

		response := message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate log is up to date and replica has not voted yet, should grant vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		env.Bus.RequestVote(candidate.ReplicaAddress(), replica.ReplicaAddress(), types.RequestVoteInput{
			CandidateID:           candidate.config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})

		env.Network.Tick()

		replica.handleMessages()

		env.Network.Tick()

		message, err := env.Bus.Receive(candidate.ReplicaAddress())
		assert.NoError(t, err)

		response := message.(*types.RequestVoteOutput)
		assert.True(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})

	t.Run("candidate log is up to date and replica has already voted for the candidate, should grant vote to same candidate again", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidate := env.Replicas[0]
		replica := env.Replicas[1]

		for i := 0; i < 2; i++ {
			env.Bus.RequestVote(candidate.ReplicaAddress(), replica.ReplicaAddress(), types.RequestVoteInput{
				CandidateID:           candidate.config.ReplicaID,
				CandidateTerm:         0,
				CandidateLastLogIndex: 0,
				CandidateLastLogTerm:  0,
			})

			env.Network.Tick()

			replica.handleMessages()

			env.Network.Tick()

			message, err := env.Bus.Receive(candidate.ReplicaAddress())
			assert.NoError(t, err)

			response := message.(*types.RequestVoteOutput)
			assert.True(t, response.VoteGranted)
			assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
		}
	})

	t.Run("candidate log is up to date but replica voted for another candidate already, should not grant vote", func(t *testing.T) {
		t.Parallel()

		env := Setup()

		candidateA := env.Replicas[0]
		candidateB := env.Replicas[1]
		replica := env.Replicas[2]

		// Request vote for candidate A.
		env.Bus.RequestVote(candidateA.ReplicaAddress(), replica.ReplicaAddress(), types.RequestVoteInput{
			CandidateID:           candidateA.config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})

		env.Network.Tick()

		replica.handleMessages()

		env.Network.Tick()

		message, err := env.Bus.Receive(candidateA.ReplicaAddress())
		assert.NoError(t, err)

		response := message.(*types.RequestVoteOutput)
		assert.True(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)

		// Request vote for candidate B
		env.Bus.RequestVote(candidateB.ReplicaAddress(), replica.ReplicaAddress(), types.RequestVoteInput{
			CandidateID:           candidateB.config.ReplicaID,
			CandidateTerm:         0,
			CandidateLastLogIndex: 0,
			CandidateLastLogTerm:  0,
		})

		env.Network.Tick()

		replica.handleMessages()

		env.Network.Tick()

		message, err = env.Bus.Receive(candidateB.ReplicaAddress())
		assert.NoError(t, err)

		response = message.(*types.RequestVoteOutput)
		assert.False(t, response.VoteGranted)
		assert.Equal(t, replica.mutableState.currentTermState.term, response.CurrentTerm)
	})
}
