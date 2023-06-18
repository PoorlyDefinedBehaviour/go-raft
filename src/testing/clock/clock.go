package testingclock

type Clock struct {
	ticks uint64
}

func NewClock() *Clock {
	return &Clock{ticks: 0}
}

func (clock *Clock) Tick() {
	clock.ticks++
}

func (clock *Clock) CurrentTick() uint64 {
	return clock.ticks
}
