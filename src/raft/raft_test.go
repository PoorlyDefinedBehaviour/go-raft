package raft

import (
	"testing"
	"time"

	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/stretchr/testify/assert"
)

func TestLeaderElectionTimeoutFired(t *testing.T) {
	cases := []struct {
		description        string
		config             Config
		initialCurrentTick uint64
		expected           bool
	}{
		{
			description:        "leader election timeout is set to a tick greater than the current tick, should return false",
			config:             Config{LeaderElectionTimeout: 5 * time.Second},
			initialCurrentTick: 0,
			expected:           false,
		},
		{
			description:        "leader election timeout is set to a tick smaller than the current tick, should return true",
			config:             Config{LeaderElectionTimeout: 5 * time.Second},
			initialCurrentTick: 5001,
			expected:           true,
		},
	}

	for _, tt := range cases {
		raft := NewRaft(tt.config, messagebus.NewMessageBus(nil))
		raft.mutableState.currentTick = tt.initialCurrentTick
		actual := raft.leaderElectionTimeoutFired()
		assert.Equal(t, tt.expected, actual, tt.description)
	}
}
