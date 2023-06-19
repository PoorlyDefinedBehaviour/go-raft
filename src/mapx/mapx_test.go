package mapx

import (
	"sort"
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

func TestKeys(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		input := rapid.SliceOfDistinct(rapid.Int64(), func(x int64) int64 { return x }).Draw(t, "input")

		m := make(map[int64]int64, len(input))

		for i, value := range input {
			m[value] = int64(i)
		}

		keys := Keys(m)

		sort.Slice(input, func(i, j int) bool { return input[i] < input[j] })
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

		assert.Equal(t, input, keys)
	})
}
