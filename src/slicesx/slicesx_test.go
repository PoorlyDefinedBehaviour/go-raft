package slicesx

import (
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/set"
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

func TestFind(t *testing.T) {
	t.Parallel()

	t.Run("property", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			input := rapid.SliceOf(rapid.Int()).Draw(t, "input")

			set := set.New[int]()
			for _, element := range input {
				set.Insert(element)
			}

			valueToFind := rapid.Int().Draw(t, "valueToFind")

			value, found := Find(input, func(x *int) bool { return *x == valueToFind })

			assert.Equal(t, set.Contains(valueToFind), found)
			if found {
				assert.Equal(t, valueToFind, *value)
			}
		})
	})

	t.Run("find returns a pointer to the element", func(t *testing.T) {
		t.Parallel()
		xs := []int{1, 2, 3}

		x, found := Find(xs, func(value *int) bool { return *value == 2 })

		assert.True(t, found)

		assert.Equal(t, 2, xs[1])

		*x += 1

		assert.Equal(t, 3, xs[1])
	})
}
