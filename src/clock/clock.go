package clock

type Clock interface {
	// Advances the clock time if needed.
	Tick()

	// Returns the tick that represents the current clock time.
	CurrentTick() uint64
}
