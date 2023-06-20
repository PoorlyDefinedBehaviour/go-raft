package timeout

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestTimeout(t *testing.T) {
	t.Parallel()

	const (
		OpTick              = "tick"
		OpReset             = "reset"
		OpFired             = "fired"
		OpResetAndFireAfter = "resetAndFireAfter"
	)

	rapid.Check(t, func(t *rapid.T) {
		const maxAfter = 1_000_000

		after := rapid.Uint64Range(0, maxAfter).Draw(t, "after")

		timeout := New(after)

		ops := rapid.SliceOf(rapid.SampledFrom([]string{OpTick, OpReset, OpFired})).Draw(t, "ops")

		ticks := uint64(0)

		for _, op := range ops {
			switch op {
			case OpTick:
				timeout.Tick()
				ticks++
			case OpReset:
				timeout.Reset()

				shouldStartFired := after == 0
				assert.Equal(t, shouldStartFired, timeout.Fired())

				ticks = 0

			case OpResetAndFireAfter:
				after = rapid.Uint64Range(0, maxAfter).Draw(t, "newAfter")
				timeout.ResetAndFireAfter(after)
			case OpFired:
				assert.Equal(t, ticks >= after, timeout.Fired())
			default:
				panic(fmt.Sprintf("unknown operation: %s", op))
			}
		}
	})
}
