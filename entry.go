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
	"sync/atomic"
	"unsafe"
)

type entry struct {
	frequency atomic.Int32
	access    atomic.Int32

	hash uint64
	next *byte

	size int
}

var entrySize = int(unsafe.Sizeof(entry{}))

type entryQueue struct {
	fifo *queue[*entry]
	size atomic.Int64
}

func newEntryQueue(cap int) *entryQueue {
	return &entryQueue{
		fifo: newQueue[*entry](cap),
	}
}

func (q *entryQueue) Push(e *entry) bool {
	size := e.size
	if q.fifo.TryEnqueue(e) {
		q.size.Add(int64(size))
		return true
	}

	return false
}

func (q *entryQueue) Pop() (*entry, bool) {
	if e, ok := q.fifo.TryDequeue(); ok {
		q.size.Add(-int64(e.size))
		return e, true
	}
	return nil, false
}

func (q *entryQueue) IsFull() bool {
	return q.size.Load() >= int64(q.fifo.cap)
}
