package ringbuffer

import (
	"errors"
	"fmt"
)

var (
	ErrRingFull = errors.New("the ring buffer is full")
)

type entry[T any] struct {
	set   bool
	value T
}

type RingBuffer[T any] struct {
	head  int
	tail  int
	size  int
	items []entry[T]
}

func New[T any](size int) (*RingBuffer[T], error) {
	if size <= 0 {
		return nil, fmt.Errorf("size must be greater than 0")
	}

	return &RingBuffer[T]{
		head:  0,
		tail:  0,
		items: make([]entry[T], size),
	}, nil
}

func (ring *RingBuffer[T]) isFull() bool {
	return ring.size == len(ring.items)
}

func (ring *RingBuffer[T]) isEmpty() bool {
	return ring.size == 0
}

func (ring *RingBuffer[T]) Push(value T) error {
	if ring.isFull() {
		return ErrRingFull
	}

	ring.items[ring.tail] = entry[T]{set: true, value: value}
	ring.tail = (ring.tail + 1) % len(ring.items)
	ring.size++

	return nil
}

func (ring *RingBuffer[T]) Pop() (T, bool) {
	if ring.isEmpty() {
		var v T
		return v, false
	}

	entry := &ring.items[ring.head]
	entry.set = false
	ring.head = (ring.head + 1) % len(ring.items)
	ring.size--

	return entry.value, true
}

func (ring *RingBuffer[T]) Find(predicate func(*T) bool) (T, bool) {
	var zeroValue T

	if ring.isEmpty() {
		return zeroValue, false
	}

	for i := 0; i < len(ring.items); i++ {
		if ring.items[i].set && predicate(&ring.items[i].value) {
			return ring.items[i].value, true
		}
	}
	return zeroValue, false
}
