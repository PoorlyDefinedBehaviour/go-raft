package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntryIsHeartbeatEntry(t *testing.T) {
	t.Parallel()

	cases := []struct {
		description string
		entry       Entry
		expected    bool
	}{
		{
			description: "entry is not a heartbeat entry, returns false",
			entry:       Entry{Type: HeartbeatEntryType + 1},
			expected:    false,
		},
		{
			description: "entry is a heartbeat entry, returns true",
			entry:       Entry{Type: HeartbeatEntryType},
			expected:    true,
		},
	}

	for _, tt := range cases {
		assert.Equal(t, tt.expected, tt.entry.IsHeartbeatEntry(), tt.description)
	}
}
