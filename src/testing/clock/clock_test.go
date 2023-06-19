package testingclock

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestClock(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		n := rapid.Uint64Range(0, 10_000).Draw(t, "n")

		clock := NewClock()

		assert.Equal(t, uint64(0), clock.CurrentTick())

		for i := 0; i < int(n); i++ {
			clock.Tick()
		}

		assert.Equal(t, n, clock.CurrentTick())
	})
}
