package rand

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenBetween(t *testing.T) {
	t.Parallel()

	t.Run("GenBetween is non inclusive", func(t *testing.T) {
		t.Parallel()

		rand := NewRand(0)

		for i := 0; i < 1000; i++ {
			assert.Equal(t, uint64(0), rand.GenBetween(0, 1))
		}
	})

	t.Run("generates different values", func(t *testing.T) {
		t.Parallel()

		rand := NewRand(0)

		values := make(map[uint64]int)

		for i := 0; i < 1000; i++ {
			values[rand.GenBetween(0, 2)] = i
		}

		assert.Equal(t, 2, len(values))
	})
}
