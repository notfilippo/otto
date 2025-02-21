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
 
// Modified version of https://github.com/puzpuzpuz/xsync/blob/main/mpmcqueue.go
// Licensed under Apache-2.0 Copyright 2025 Andrei Pechkurov

package otto

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// Based on the data structure from the following C++ library:
// https://github.com/rigtorp/MPMCQueue

type queue[I any] struct {
	cap  uint64
	size atomic.Int64

	head uint64
	hpad [cacheLineSize - 8]byte
	tail uint64
	tpad [cacheLineSize - 8]byte

	slots []slotOfPadded[I]
}

type slotOfPadded[I any] struct {
	slotOf[I]
	// Unfortunately, proper padding like the below one:
	//
	// pad [cacheLineSize - (unsafe.Sizeof(slotOf[I]{}) % cacheLineSize)]byte
	//
	// won't compile, so here we add a best-effort padding for items up to
	// 56 bytes size.
	//
	// For this specific use case we can consider item to be a pointer.
	pad [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})*2 - unsafe.Sizeof(uintptr(0))]byte
}

type slotOf[I any] struct {
	// atomic.Uint64 is used here to get proper 8 byte alignment on
	// 32-bit archs.
	turn atomic.Uint64
	size atomic.Int64
	item I
}

func newQueue[I any](capacity int) *queue[I] {
	if capacity < 1 {
		panic("capacity must be positive number")
	}
	return &queue[I]{
		cap:   uint64(capacity),
		slots: make([]slotOfPadded[I], capacity),
	}
}

// TryPush inserts the given item into the queue. Does not block
// and returns immediately. The result indicates that the queue isn't
// full and the item was inserted.
func (q *queue[I]) TryPush(item I, size int) bool {
	head := atomic.LoadUint64(&q.head)
	slot := &q.slots[q.idx(head)]
	turn := q.turn(head) * 2
	if slot.turn.Load() == turn {
		if atomic.CompareAndSwapUint64(&q.head, head, head+1) {
			slot.item = item
			slot.turn.Store(turn + 1)
			slot.size.Store(int64(size))
			q.size.Add(int64(size))
			return true
		}
	}
	return false
}

func (q *queue[I]) MustPush(item I, size int) {
	for !q.TryPush(item, size) {
		runtime.Gosched()
	}
}

// TryPop retrieves and removes the item from the head of the
// queue. Does not block and returns immediately. The ok result
// indicates that the queue isn't empty and an item was retrieved.
func (q *queue[I]) TryPop() (item I, ok bool) {
	tail := atomic.LoadUint64(&q.tail)
	slot := &q.slots[q.idx(tail)]
	turn := q.turn(tail)*2 + 1
	if slot.turn.Load() == turn {
		if atomic.CompareAndSwapUint64(&q.tail, tail, tail+1) {
			var zeroI I
			item = slot.item
			ok = true
			slot.item = zeroI
			q.size.Add(-slot.size.Swap(0))
			slot.turn.Store(turn + 1)
			return
		}
	}
	return
}

func (q *queue[I]) Size() int {
	return int(q.size.Load())
}

func (q *queue[I]) idx(i uint64) uint64 {
	return i % q.cap
}

func (q *queue[I]) turn(i uint64) uint64 {
	return i / q.cap
}
