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

// Modified version of https://github.com/maypok86/otter/blob/main/internal/queue/growable.go
// Licensed under Apache-2.0 Copyright 2024 Alexey Mayshev

package otto

import (
	"sync"
)

type taskQueue struct {
	mutex sync.Mutex

	notEmpty sync.Cond
	notFull  sync.Cond

	buf []task

	head int
	tail int

	count  int
	minCap int
	maxCap int
}

func newTaskQueue(minCap, maxCap int) *taskQueue {
	g := &taskQueue{
		buf:    make([]task, minCap),
		minCap: minCap,
		maxCap: maxCap,
	}

	g.notEmpty = sync.Cond{L: &g.mutex}
	g.notFull = sync.Cond{L: &g.mutex}

	return g
}

// Push adds a task to the queue.
func (g *taskQueue) Push(item task) {
	g.mutex.Lock()
	for g.count == g.maxCap {
		g.notFull.Wait()
	}
	g.push(item)
	g.mutex.Unlock()
}

func (g *taskQueue) push(item task) {
	g.grow()
	g.buf[g.tail] = item
	g.tail = g.next(g.tail)
	g.count++
	g.notEmpty.Signal()
}

// Pop removes and returns a task from the queue.
func (g *taskQueue) Pop() task {
	g.mutex.Lock()
	for g.count == 0 {
		g.notEmpty.Wait()
	}
	item := g.pop()
	g.mutex.Unlock()
	return item
}

// TryPop removes and returns a task from the queue.
func (g *taskQueue) TryPop() (task, bool) {
	var zero task
	g.mutex.Lock()
	if g.count == 0 {
		g.mutex.Unlock()
		return zero, false
	}
	item := g.pop()
	g.mutex.Unlock()
	return item, true
}

func (g *taskQueue) pop() task {
	var zero task

	item := g.buf[g.head]
	g.buf[g.head] = zero

	g.head = g.next(g.head)
	g.count--

	g.notFull.Signal()

	return item
}

func (g *taskQueue) Clear() {
	g.mutex.Lock()
	for g.count > 0 {
		g.pop()
	}
	g.mutex.Unlock()
}

func (g *taskQueue) Empty() bool {
	g.mutex.Lock()
	empty := g.count == 0
	g.mutex.Unlock()
	return empty
}

func (g *taskQueue) grow() {
	if g.count != len(g.buf) {
		return
	}
	g.resize()
}

func (g *taskQueue) resize() {
	newBuf := make([]task, g.count<<1)
	if g.tail > g.head {
		copy(newBuf, g.buf[g.head:g.tail])
	} else {
		n := copy(newBuf, g.buf[g.head:])
		copy(newBuf[n:], g.buf[:g.tail])
	}

	g.head = 0
	g.tail = g.count
	g.buf = newBuf
}

func (g *taskQueue) next(i int) int {
	return (i + 1) & (len(g.buf) - 1)
}
