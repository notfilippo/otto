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
	"bytes"
	"sync"
	"sync/atomic"
)

type queueType uint8

const (
	queueWindow queueType = iota
	queueProbation
	queueProtected
)

type entry struct {
	hash       uint64
	dead       atomic.Bool
	queue      queueType
	next, prev *entry
	buf        bytes.Buffer
	bufLock    sync.RWMutex
}

func (e *entry) reset() {
	e.hash = 0
	e.dead.Store(false)
	e.queue = queueWindow
	e.next = nil
	e.prev = nil
	e.buf.Reset()
}

func (e *entry) weight() int {
	return e.buf.Len()
}

type entryDeque struct {
	head, tail *entry
}

func (q *entryDeque) Contains(e *entry) bool {
	return e.prev != nil || e.next != nil || e == q.head
}

func (q *entryDeque) Remove(e *entry) {
	if q.Contains(e) {
		q.unlink(e)
	}
}

func (q *entryDeque) MoveBack(e *entry) {
	if q.Contains(e) && e != q.tail {
		q.unlink(e)
		q.pushBack(e)
	}
}

func (q *entryDeque) OfferFront(e *entry) {
	if !q.Contains(e) {
		q.pushFront(e)
	}
}

func (q *entryDeque) OfferBack(e *entry) {
	if !q.Contains(e) {
		q.pushBack(e)
	}
}

func (q *entryDeque) pushFront(e *entry) {
	if q.head == nil {
		q.head = e
		q.tail = e
	} else {
		e.next = q.head
		q.head.prev = e
		q.head = e
	}
}

func (q *entryDeque) pushBack(e *entry) {
	if q.tail == nil {
		q.head = e
		q.tail = e
	} else {
		e.prev = q.tail
		q.tail.next = e
		q.tail = e
	}
}

func (q *entryDeque) unlink(e *entry) {
	if e.prev == nil {
		q.head = e.next
	} else {
		e.prev.next = e.next
		e.prev = nil
	}

	if e.next == nil {
		q.tail = e.prev
	} else {
		e.next.prev = e.prev
		e.next = nil
	}
}
