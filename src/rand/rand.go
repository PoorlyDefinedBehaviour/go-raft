package rand

import (
	"math/rand"
)

type Random interface {
	// Generates a boolean with probability `p` of it being true.
	GenBool(p float64) bool

	GenBetween(min, max uint64) uint64
}

type DefaultRandom struct {
	rand *rand.Rand
}

func NewRand(seed int64) *DefaultRandom {
	source := rand.NewSource(seed)
	rand := rand.New(source)
	return &DefaultRandom{rand: rand}
}

// Generates a boolean with probability `p` of it being true.
func (rand *DefaultRandom) GenBool(p float64) bool {
	return p < rand.rand.Float64()
}

func (rand *DefaultRandom) GenBetween(min, max uint64) uint64 {
	if max == 0 {
		return 0
	}
	return min + rand.rand.Uint64()%max
}
