package slicesx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func TestMap(t *testing.T) {
	t.Parallel()

	double := func(x int) int {
		return x * 2
	}

	rapid.Check(t, func(t *rapid.T) {
		input := rapid.SliceOf(rapid.Int()).Draw(t, "input")

		expected := make([]int, 0, len(input))
		for _, value := range input {
			expected = append(expected, double(value))
		}

		actual := Map(input, func(x *int) int {
			return double(*x)
		})

		assert.Equal(t, expected, actual)
	})
}
