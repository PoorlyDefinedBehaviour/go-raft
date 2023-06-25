package timeout

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

type Model struct {
	after uint64
	ticks uint64
}

func (model *Model) Tick() {
	model.ticks++
}

func (model *Model) Reset() {
	model.ticks = 0
}

func (model *Model) ResetAndFireAfter(after uint64) {
	model.Reset()
	model.after = after
}

func newModel(after uint64) *Model {
	return &Model{after: after, ticks: 0}
}

func TestTimeout(t *testing.T) {
	t.Parallel()

	const (
		OpTick              = "tick"
		OpReset             = "reset"
		OpFired             = "fired"
		OpResetAndFireAfter = "resetAndFireAfter"
		OpTicks             = "ticks"
		OpAfter             = "after"
	)

	rapid.Check(t, func(t *rapid.T) {
		const maxAfter = 1_000_000

		after := rapid.Uint64Range(0, maxAfter).Draw(t, "after")

		timeout := New(after)
		model := newModel(after)

		ops := rapid.SliceOf(rapid.SampledFrom([]string{OpTick, OpReset, OpFired, OpResetAndFireAfter, OpTicks, OpAfter})).Draw(t, "ops")

		for _, op := range ops {
			switch op {
			case OpTick:
				timeout.Tick()
				model.Tick()

			case OpReset:
				timeout.Reset()
				model.Reset()

				shouldStartFired := after == 0
				assert.Equal(t, shouldStartFired, timeout.Fired())

			case OpResetAndFireAfter:
				after = rapid.Uint64Range(0, maxAfter).Draw(t, "newAfter")
				timeout.ResetAndFireAfter(after)
				model.ResetAndFireAfter(after)

			case OpFired:
				assert.Equal(t, model.ticks >= after, timeout.Fired())

			case OpTicks:
				assert.Equal(t, model.ticks, timeout.Ticks())

			case OpAfter:
				assert.Equal(t, model.after, timeout.After())

			default:
				panic(fmt.Sprintf("unknown operation: %s", op))
			}
		}
	})
}
