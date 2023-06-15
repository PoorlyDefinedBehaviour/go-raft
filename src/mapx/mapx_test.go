package mapx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestMinValue(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		input := rapid.SliceOf(rapid.Int64()).Draw(t, "input")

		// Fill the map with the input values.
		m := make(map[int64]int64, len(input))
		for i, value := range input {
			m[int64(i)] = value
		}

		actual, ok := MinValue(m)

		// If the input is empty.
		if len(input) == 0 {
			assert.False(t, ok)
		} else {
			// If the input is not empty, find the min value in the input.
			min := input[0]
			for _, value := range input {
				if value < min {
					min = value
				}

			}

			// Ensure the value returned by MinValue is the min value in the input.
			assert.True(t, ok)
			assert.Equal(t, min, actual)
		}
	})
}
