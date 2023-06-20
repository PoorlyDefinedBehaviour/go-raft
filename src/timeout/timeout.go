package timeout

type T struct {
	ticks uint64
	after uint64
}

func New(timesOutAtTick uint64) T {
	return T{after: timesOutAtTick}
}

func (timeout *T) Tick() {
	timeout.ticks++
}

func (timeout *T) Reset() {
	timeout.ticks = 0
}

func (timeout *T) ResetAndFireAfter(after uint64) {
	timeout.Reset()
	timeout.after = after
}

func (timeout *T) Fired() bool {
	return timeout.ticks >= timeout.after
}
