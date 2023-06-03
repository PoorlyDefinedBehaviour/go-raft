package simulator_test

import (
	"container/heap"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*MessageToSend

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest, priority so we use lowest than here.
	return pq[i].AfterTick < pq[j].AfterTick
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*MessageToSend)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *MessageToSend, message types.Message, priority int) {
	item.Message = message
	item.AfterTick = uint64(priority)
	heap.Fix(pq, item.Index)
}
