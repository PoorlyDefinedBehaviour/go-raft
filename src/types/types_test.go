package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntryIsSystemEntry(t *testing.T) {
	t.Parallel()

	for i := 1; i <= MaxReservedEntryType; i++ {
		entry := Entry{Type: uint8(i)}
		assert.True(t, entry.IsSystemEntry())
	}

	for i := MaxReservedEntryType + 1; i < MaxReservedEntryType+10; i++ {
		entry := Entry{Type: uint8(i)}
		assert.False(t, entry.IsSystemEntry())
	}
}
