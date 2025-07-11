// Copyright 2025 Filippo Rossi
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Modified version of https://github.com/puzpuzpuz/xsync/blob/main/mpmcqueueof.go
// Licensed under Apache-2.0 Copyright 2025 Andrei Pechkurov

package otto

import (
	"iter"
	"sync/atomic"
	"unsafe"
)

// A queue is a bounded multi-producer multi-consumer concurrent
// queue.
//
// queue instances must be created with newQueue function.
// A queue must not be copied after first use.
//
// Based on the data structure from the following C++ library:
// https://github.com/rigtorp/MPMCQueue
type queue[T any] struct {
	cap  uint64
	head atomic.Uint64
	//lint:ignore U1000 prevents false sharing
	hpad [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte
	tail atomic.Uint64
	//lint:ignore U1000 prevents false sharing
	tpad  [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte
	slots []slotPadded[T]
}

type slotPadded[T any] struct {
	slot[T]
	//lint:ignore U1000 prevents false sharing
	pad [cacheLineSize - unsafe.Sizeof(slot[uintptr]{})]byte
}

type slot[T any] struct {
	turn atomic.Uint64
	item T
}

// newQueue creates a new Queue instance with the given
// capacity.
func newQueue[T any](cap int) *queue[T] {
	if cap < 1 {
		panic("capacity must be positive number")
	}
	return &queue[T]{
		cap:   uint64(cap),
		slots: make([]slotPadded[T], cap),
	}
}

// TryEnqueue inserts the given item into the queue. Does not block
// and returns immediately. The result indicates that the queue isn't
// full and the item was inserted.
func (q *queue[T]) TryEnqueue(item T) bool {
	head := q.head.Load()
	slot := &q.slots[q.idx(head)]
	turn := q.turn(head) * 2
	if slot.turn.Load() == turn {
		if q.head.CompareAndSwap(head, head+1) {
			slot.item = item
			slot.turn.Store(turn + 1)
			return true
		}
	}
	return false
}

// TryDequeue retrieves and removes the item from the head of the
// queue. Does not block and returns immediately. The ok result
// indicates that the queue isn't empty and an item was retrieved.
func (q *queue[T]) TryDequeue() (item T, ok bool) {
	tail := q.tail.Load()
	slot := &q.slots[q.idx(tail)]
	turn := q.turn(tail)*2 + 1
	if slot.turn.Load() == turn {
		if q.tail.CompareAndSwap(tail, tail+1) {
			item = slot.item
			ok = true
			var zero T
			slot.item = zero
			slot.turn.Store(turn + 1)
			return
		}
	}
	return
}

// TryDequeueBatch attempts to atomically dequeue exactly n items from the queue.
// Returns the dequeued items and a boolean indicating success. If the queue
// doesn't have at least n items, no items are dequeued and the function returns false.
func (q *queue[T]) TryDequeueBatch(n int, yield func(item T)) bool {
	if n <= 0 {
		return false
	}

	// Load the tail index
	tail := q.tail.Load()

	// Check if all n slots are ready for dequeuing
	for i := range n {
		idx := q.idx(tail + uint64(i))
		slot := &q.slots[idx]
		turn := q.turn(tail+uint64(i))*2 + 1
		if slot.turn.Load() != turn {
			return false // Not all slots are ready
		}
	}

	// Try to atomically increment the tail index by n
	if !q.tail.CompareAndSwap(tail, tail+uint64(n)) {
		return false // Another consumer modified tail
	}

	// Dequeue all n items
	for i := range n {
		idx := q.idx(tail + uint64(i))
		slot := &q.slots[idx]
		turn := q.turn(tail+uint64(i))*2 + 1

		yield(slot.item)
		var zero T
		slot.item = zero
		slot.turn.Store(turn + 1)
	}

	return true
}

func (q *queue[T]) TryEnqueueBatch(n int, provide func() T) bool {
	if n <= 0 {
		return false
	}

	// Lad the head index
	head := q.head.Load()

	// Check if all n slots are ready for enqueuing
	for i := range n {
		idx := q.idx(head + uint64(i))
		slot := &q.slots[idx]
		turn := q.turn(head+uint64(i)) * 2
		if slot.turn.Load() != turn {
			return false // Not all slots are ready
		}
	}

	// Try to atomically increment the head index by n
	if !q.head.CompareAndSwap(head, head+uint64(n)) {
		return false // Another consumer modified head
	}

	// Enqueue all n items
	for i := range n {
		idx := q.idx(head + uint64(i))
		slot := &q.slots[idx]
		turn := q.turn(head+uint64(i)) * 2

		slot.item = provide()
		slot.turn.Store(turn + 1)
	}

	return true
}

func (q *queue[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for i := q.tail.Load(); i != q.head.Load(); i++ {
			if !yield(q.slots[q.idx(i)].item) {
				return
			}
		}
	}
}

func (q *queue[T]) Len() int {
	head := q.head.Load()
	tail := q.tail.Load()

	if head == tail {
		if q.slots[q.idx(head)].turn == q.slots[q.idx(tail)].turn {
			return 0
		}
		return 1
	}

	return int(head - tail)
}

func (q *queue[T]) idx(i uint64) uint64 {
	return i % q.cap
}

func (q *queue[T]) turn(i uint64) uint64 {
	return i / q.cap
}
