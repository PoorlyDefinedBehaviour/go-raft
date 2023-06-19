package ringbuffer

import (
	"errors"
	"fmt"
)

var (
	ErrRingFull = errors.New("the ring buffer is full")
)

type RingBuffer[T any] struct {
	start int
	end   int
	size  int
	items []T
}

func New[T any](size int) (*RingBuffer[T], error) {
	if size <= 0 {
		return nil, fmt.Errorf("size must be greater than 0")
	}

	return &RingBuffer[T]{
		start: 0,
		end:   0,
		items: make([]T, size),
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

	ring.items[ring.end] = value
	ring.end = (ring.end + 1) % len(ring.items)
	ring.size++

	return nil
}

func (ring *RingBuffer[T]) Pop() (T, bool) {
	if ring.isEmpty() {
		var v T
		return v, false
	}

	value := ring.items[ring.start]
	ring.start = (ring.start + 1) % len(ring.items)
	ring.size--

	return value, true
}
