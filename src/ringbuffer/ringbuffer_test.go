package ringbuffer

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

type Model[T any] struct {
	size  int
	items []T
}

func newModel[T any](size int) *Model[T] {
	return &Model[T]{size: size}
}

func (model *Model[T]) isFull() bool {
	return len(model.items) == model.size
}

func (model *Model[T]) Push(value T) {
	model.items = append(model.items, value)
}

func (model *Model[T]) Pop() (T, bool) {
	var value T

	if len(model.items) == 0 {
		return value, false
	}

	value = model.items[0]
	model.items = model.items[1:]

	return value, true
}

func (model *Model[T]) Find(predicate func(*T) bool) (T, bool) {
	for i := 0; i < len(model.items); i++ {
		if predicate(&model.items[i]) {
			return model.items[i], true
		}
	}

	var zeroValue T
	return zeroValue, false
}

func TestRingBuffer(t *testing.T) {
	t.Parallel()

	const (
		OpPush = "push"
		OpPop  = "pop"
		OpFind = "find"
	)

	rapid.Check(t, func(t *rapid.T) {
		ringSize := rapid.IntRange(1, 100_000).Draw(t, "ringSize")

		ring, err := New[int64](int(ringSize))
		assert.NoError(t, err)

		model := newModel[int64](ringSize)

		operations := rapid.SliceOf(rapid.SampledFrom([]string{OpPush, OpPop, OpFind})).Draw(t, "operations")

		for _, operation := range operations {
			switch operation {
			case OpPush:
				value := rapid.Int64().Draw(t, "valueToPush")
				err := ring.Push(value)
				if err != nil {
					assert.True(t, model.isFull())
					assert.True(t, errors.Is(err, ErrRingFull))
				} else {
					model.Push(value)
				}

			case OpPop:
				value, ok := ring.Pop()
				modelValue, modelOk := model.Pop()

				assert.Equal(t, modelOk, ok)
				assert.Equal(t, modelValue, value)

			case OpFind:
				valueToFind := rapid.Int64().Draw(t, "valueToFind")

				value, found := ring.Find(func(x *int64) bool { return *x == valueToFind })
				modelValue, modelFound := model.Find(func(x *int64) bool { return *x == valueToFind })

				assert.Equal(t, value, modelValue)
				assert.Equal(t, found, modelFound)
			default:
				panic(fmt.Sprintf("unknown operation: %s", operation))
			}
		}
	})
}

func TestSimple(t *testing.T) {
	t.Parallel()

	ring, err := New[int](2)
	assert.NoError(t, err)

	_, ok := ring.Pop()
	assert.False(t, ok)

	assert.NoError(t, ring.Push(1))
	assert.NoError(t, ring.Push(2))
	assert.Equal(t, ErrRingFull, ring.Push(3))

	value, ok := ring.Pop()
	assert.True(t, ok)
	assert.Equal(t, 1, value)

	value, ok = ring.Pop()
	assert.True(t, ok)
	assert.Equal(t, 2, value)

	_, ok = ring.Pop()
	assert.False(t, ok)
}
