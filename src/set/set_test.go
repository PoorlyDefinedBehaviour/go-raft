package set

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

type Model[T comparable] struct {
	items []T
}

func newModel[T comparable]() Model[T] {
	return Model[T]{}
}

func (model *Model[T]) Insert(value T) {
	if model.Contains(value) {
		return
	}
	model.items = append(model.items, value)
}

func (model *Model[T]) Remove(value T) bool {
	for i, item := range model.items {
		if item == value {
			if i == 0 {
				model.items = model.items[1:]
			} else {
				model.items = append(model.items[0:i], model.items[i+1:]...)
			}

			return true
		}
	}

	return false
}

func (model *Model[T]) Size() int {
	return len(model.items)
}

func (model *Model[T]) Contains(value T) bool {
	for _, item := range model.items {
		if item == value {
			return true
		}
	}
	return false
}

func (model *Model[T]) Find(predicate func(*T) bool) (T, bool) {
	for _, item := range model.items {
		if predicate(&item) {
			return item, true
		}
	}

	var zeroValue T
	return zeroValue, false
}

func (model *Model[T]) Retain(predicate func(*T) bool) {
	items := make([]T, 0)

	for _, item := range model.items {
		if predicate(&item) {
			items = append(items, item)
		}
	}

	model.items = items
}

func maybeExistingMember[T comparable](t *rapid.T, model *Model[T]) T {
	var member T
	if model.Size() == 0 {
		return member
	}

	return model.items[rapid.IntRange(0, len(model.items)-1).Draw(t, "index")]
}

func TestSetModel(t *testing.T) {
	t.Parallel()

	const (
		OpInsert   = "insert"
		OpRemove   = "remove"
		OpContains = "contains"
		OpFind     = "find"
		OpRetain   = "retain"
		OpSize     = "size"
	)

	rapid.Check(t, func(t *rapid.T) {
		ops := rapid.SliceOf(rapid.SampledFrom([]string{OpInsert, OpRemove, OpContains, OpFind, OpRetain, OpSize})).Draw(t, "ops")

		model := newModel[int]()

		set := New[int]()

		for _, op := range ops {
			switch op {
			case OpInsert:
				value := rapid.Int().Draw(t, "input")
				set.Insert(value)
				model.Insert(value)

			case OpRemove:
				value := maybeExistingMember(t, &model)

				removedFromSet := set.Remove(value)
				removedFromModel := model.Remove(value)
				assert.Equal(t, removedFromModel, removedFromSet)

			case OpContains:
				value := maybeExistingMember(t, &model)
				assert.Equal(t, model.Contains(value), set.Contains(value))

			case OpFind:
				value := maybeExistingMember(t, &model)

				modelValue, modelFound := model.Find(func(x *int) bool { return *x == value })
				setValue, setFound := set.Find(func(x *int) bool { return *x == value })

				assert.Equal(t, modelValue, setValue)
				assert.Equal(t, modelFound, setFound)

			case OpRetain:
				value := maybeExistingMember(t, &model)

				model.Retain(func(x *int) bool { return *x <= value })
				set.Retain(func(x *int) bool { return *x <= value })

			case OpSize:
				assert.Equal(t, model.Size(), set.Size())

			default:
				panic(fmt.Sprintf("unexpected op: %s", op))
			}
		}
	})
}

func TestSet(t *testing.T) {
	t.Parallel()

	set := New[int]()

	assert.False(t, set.Contains(1))

	set.Insert(1)

	assert.True(t, set.Contains(1))

	set.Insert(2)
	set.Insert(1)

	assert.False(t, set.Contains(3))

	assert.True(t, set.Contains(2))

	assert.False(t, set.Remove(4))

	assert.True(t, set.Remove(2))
	assert.False(t, set.Contains(2))

	assert.True(t, set.Contains(1))
}
