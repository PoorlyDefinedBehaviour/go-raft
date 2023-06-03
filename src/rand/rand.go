package rand

type Random interface {
	// Generates a boolean with probability `p` of it being true.
	GenBool(p float64) bool

	GenBetween(min, max uint64) uint64
}

type DefaultRandom struct{}

func NewRand(seed uint64) *DefaultRandom {
	return &DefaultRandom{}
}

// Generates a boolean with probability `p` of it being true.
func (rand *DefaultRandom) GenBool(p float64) bool {
	return false
}

func (rand *DefaultRandom) GenBetween(min, max uint64) uint64 {
	return 0
}
