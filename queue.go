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

package otto

import (
	"sync"
	"sync/atomic"
)

// https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html

type queue[T any] struct {
	head, tail atomic.Pointer[node[T]]
	size       atomic.Int64
	pool       sync.Pool
}

type node[T any] struct {
	next  atomic.Pointer[node[T]]
	value T
	size  int
}

func newQueue[T any]() *queue[T] {
	n := &node[T]{}
	f := &queue[T]{pool: sync.Pool{New: func() interface{} { return &node[T]{} }}}
	f.head.Store(n)
	f.tail.Store(n)
	return f
}

func (q *queue[T]) Push(value T, size int) {
	n := q.pool.Get().(*node[T])
	n.next.Store(nil)
	n.value = value
	n.size = size

	for {
		tail := q.tail.Load()
		next := tail.next.Load()
		if tail == q.tail.Load() {
			if next == nil {
				if tail.next.CompareAndSwap(next, n) {
					q.tail.CompareAndSwap(tail, n)
					q.size.Add(int64(size))
					return
				}
			} else {
				q.tail.CompareAndSwap(tail, next)
			}
		}
	}
}

func (q *queue[T]) Pop() T {
	for {
		head := q.head.Load()
		tail := q.tail.Load()
		next := head.next.Load()
		if head == q.head.Load() {
			if head == tail {
				if next == nil {
					var zero T
					return zero
				}
				q.tail.CompareAndSwap(tail, next)
			} else {
				n := next
				if q.head.CompareAndSwap(head, next) {
					q.size.Add(-int64(n.size))
					q.pool.Put(head)
					return n.value
				}
			}
		}
	}
}

func (q *queue[T]) Size() int {
	return int(q.size.Load())
}
