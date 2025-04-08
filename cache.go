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
	"hash/maphash"
	"io"
	"log"
	"math"
	"runtime"
	"sync"
)

var Debug = false

const (
	HillClimberRestartThreshold = 0.05
	HillClimberStepPercent      = 0.0625
	HillClimberStepDecayRate    = 0.98
	QueueTransferThreshold      = 1000
)

var (
	parallelism        = availableParallelism()
	roundedParallelism = int(roundUpPowerOf2(uint32(parallelism)))

	writeBufferMinCap = 4
	writeBufferMaxCap = 128 * roundedParallelism

	readBufferSize = 4 * roundedParallelism
)

type Cache interface {
	Set(key string, val []byte) bool
	Get(key string, buf []byte) ([]byte, bool)

	Clear()
	Close()

	Entries() int

	Serialize(w io.Writer) error
}

type cache struct {
	data *hmap

	writeBuffer *taskQueue
	readBuffers []*stripedBuffer

	seed maphash.Seed

	pool sync.Pool

	evictionLock    sync.Mutex
	frequencySketch *frequencySketch

	size, cap int

	windowSize, windowCap       int
	protectedSize, protectedCap int

	window    entryDeque
	protected entryDeque
	probation entryDeque

	stepSize   float64
	adjustment int

	previousSampleHitRate float64
	missesInSample        int
	hitsInSample          int

	doneClear chan struct{}
}

func New(cap int) Cache {
	writeBuffer := newTaskQueue(writeBufferMinCap, writeBufferMaxCap)

	readBuffers := make([]*stripedBuffer, 0, readBufferSize)
	for range readBufferSize {
		readBuffers = append(readBuffers, newStripedBuffer())
	}

	const PercentMain = 0.99
	const PercentMainProtected = 0.8

	windowCap := cap - int(PercentMain*float64(cap))
	protectedCap := int(PercentMainProtected * float64(cap-windowCap))

	c := &cache{
		data:         newMap(),
		writeBuffer:  writeBuffer,
		readBuffers:  readBuffers,
		seed:         maphash.MakeSeed(),
		cap:          cap,
		windowCap:    windowCap,
		protectedCap: protectedCap,
		stepSize:     -HillClimberStepPercent * float64(cap),
		doneClear:    make(chan struct{}),
	}

	c.pool = sync.Pool{
		New: func() any {
			return new(entry)
		},
	}

	go c.loop()

	return c
}

func (c *cache) Set(key string, value []byte) bool {
	weight := len(value)
	if weight > c.cap {
		return false
	}

	hash := maphash.String(c.seed, key)

	_, loaded := c.data.LoadOrCompute(hash, func() *entry {
		entry := c.pool.Get().(*entry)
		entry.reset()
		entry.hash = hash
		entry.buf.Write(value)
		c.writeBuffer.Push(newAddTask(entry))
		return entry
	})

	if Debug {
		log.Printf("interning key %d, already present = %v", hash, loaded)
	}

	return !loaded
}

func (c *cache) get(hash uint64, dst []byte) ([]byte, bool) {
	entry, ok := c.data.Load(hash)

	if Debug {
		log.Printf("getting key %d, found = %v", hash, ok)
	}

	if !ok {
		return nil, false
	}

	entry.bufLock.RLock()
	if entry.dead.Load() {
		entry.bufLock.RUnlock()
		return nil, false
	}

	if cap(dst) < entry.buf.Len() {
		dst = make([]byte, entry.buf.Len())
	}

	start := dst
	dst = append(dst, entry.buf.Bytes()...)
	entry.bufLock.RUnlock()

	c.afterRead(entry)

	return start, true
}

func (c *cache) Get(key string, dst []byte) ([]byte, bool) {
	hash := maphash.String(c.seed, key)
	return c.get(hash, dst)
}

func (c *cache) Close() {
	c.close(newCloseTask())
}

func (c *cache) Clear() {
	c.close(newClearTask())
}

func (c *cache) close(t task) {
	c.data.Clear()
	for i := range c.readBuffers {
		c.readBuffers[i].Clear()
	}

	c.writeBuffer.Push(t)
	<-c.doneClear
}

func (c *cache) loop() {
	for {
		task := c.writeBuffer.Pop()
		if Debug {
			log.Printf("executing task %v", task)
		}

		c.evictionLock.Lock()
		c.onWrite(task)
		c.evictionLock.Unlock()

		if task.kind == closeTask {
			break
		}
	}
}

func (c *cache) onWrite(t task) {
	switch t.kind {
	case clearTask, closeTask:
		c.writeBuffer.Clear()

		c.window = entryDeque{}
		c.protected = entryDeque{}
		c.probation = entryDeque{}
		c.windowSize = 0
		c.protectedSize = 0
		c.size = 0
		c.frequencySketch = nil
		c.previousSampleHitRate = 0
		c.missesInSample = 0
		c.hitsInSample = 0
		c.adjustment = 0
		c.stepSize = -HillClimberStepPercent * float64(c.cap)

		c.doneClear <- struct{}{}
	case addTask:
		c.add(t.current)
	}

	c.maintenance()
}

func (c *cache) maintenance() {
	c.evictEntries()
	c.climb()
}

func (c *cache) climb() {
	c.determineAdjustment()
	c.demoteFromProtected()
	if c.adjustment > 0 {
		c.increaseWindow()
	} else if c.adjustment < 0 {
		c.decreaseWindow()
	}
}

func (c *cache) increaseWindow() {
	if c.protectedCap == 0 {
		return
	}

	quota := min(c.adjustment, c.protectedCap)
	c.protectedCap -= quota
	c.windowCap += quota
	c.demoteFromProtected()

	for range QueueTransferThreshold {
		candidate := c.probation.head
		probation := true

		if candidate == nil || quota < candidate.weight() {
			candidate = c.protected.head
			probation = false
		}

		if candidate == nil || quota < candidate.weight() {
			break
		}

		quota -= candidate.weight()
		if probation {
			c.probation.Remove(candidate)
		} else {
			c.protectedSize -= candidate.weight()
			c.protected.Remove(candidate)
		}

		c.windowSize += candidate.weight()
		c.window.OfferBack(candidate)
		candidate.queue = queueWindow
	}

	c.protectedCap += quota
	c.windowCap -= quota
	c.adjustment = quota
}

func (c *cache) decreaseWindow() {
	if c.windowCap <= 1 {
		return
	}

	quota := min(-c.adjustment, max(0, c.windowCap-1))
	c.protectedCap += quota
	c.windowCap -= quota

	for range QueueTransferThreshold {
		candidate := c.window.head
		if candidate == nil || quota < candidate.weight() {
			break
		}

		quota -= candidate.weight()
		c.windowSize -= candidate.weight()
		c.window.Remove(candidate)
		c.probation.OfferBack(candidate)
		candidate.queue = queueProbation
	}

	c.protectedCap -= quota
	c.windowCap += quota
	c.adjustment = -quota
}

func (c *cache) demoteFromProtected() {
	if c.protectedSize <= c.protectedCap {
		return
	}

	size := c.protectedSize
	for range QueueTransferThreshold {
		if size <= c.protectedCap {
			break
		}

		demoted := c.protected.head
		if demoted == nil {
			break
		}

		c.protected.Remove(demoted)
		c.probation.OfferBack(demoted)
		demoted.queue = queueProbation

		size = demoted.weight()
	}

	c.protectedSize = size
}

func (c *cache) determineAdjustment() {
	if c.frequencySketch == nil {
		c.previousSampleHitRate = 0.
		c.missesInSample = 0
		c.hitsInSample = 0
		return
	}

	requestCount := c.hitsInSample + c.missesInSample
	if requestCount < c.frequencySketch.SampleSize() {
		return
	}

	hitRate := float64(c.hitsInSample) / float64(c.missesInSample)
	hitRateChange := hitRate - c.previousSampleHitRate

	var direction float64
	if hitRateChange >= 0 {
		direction = 1
	} else {
		direction = -1
	}

	amount := c.stepSize * direction
	c.adjustment = int(amount)

	if math.Abs(hitRateChange) >= HillClimberRestartThreshold {
		c.stepSize = HillClimberStepPercent * float64(c.cap) * direction
	} else {
		c.stepSize = HillClimberStepDecayRate * amount
	}

	c.hitsInSample = 0
	c.missesInSample = 0
	c.previousSampleHitRate = hitRate
}

func (c *cache) add(e *entry) {
	if e.dead.Load() {
		return
	}

	c.size += e.weight()
	c.windowSize += e.weight()
	if c.size >= int(uint(c.cap)>>1) {
		if c.size > c.cap {
			c.evictEntries()
		} else {
			capacity := c.data.Size()
			// Lazy-initialize the sketch (or update it if the table cannot support the capacity).
			if c.frequencySketch == nil || len(c.frequencySketch.table) < capacity {
				c.frequencySketch = newFrequencySketch(capacity)
			}
		}
	}

	c.frequencySketch.Increment(e.hash)

	if e.weight() > c.windowCap {
		c.window.OfferFront(e)
	} else {
		c.window.OfferBack(e)
	}

	c.missesInSample += 1
}

func (c *cache) evictEntries() {
	candidate := c.evictFromWindow()
	c.evictFromMain(candidate)
}

func (c *cache) evictFromWindow() *entry {
	var first *entry

	node := c.window.head
	for c.windowSize > c.windowCap {
		if node == nil {
			break
		}
		next := node.next
		if node.weight() != 0 {
			node.queue = queueProbation
			c.window.Remove(node)
			c.probation.OfferBack(node)
			if first == nil {
				first = node
			}

			c.windowSize -= node.weight()
		}
		node = next
	}

	return first
}

func (c *cache) evictFromMain(candidate *entry) {
	victimQueue := queueProbation
	candidateQueue := queueProbation
	victim := c.probation.head
	for c.size > c.cap {
		// Search the admission window for additional candidates.
		if candidate == nil && candidateQueue == queueProbation {
			candidate = c.window.head
			candidateQueue = queueWindow
		}

		// Try evicting from the protected and window queues.
		if candidate == nil && victim == nil {
			if victimQueue == queueProbation {
				victim = c.protected.head
				victimQueue = queueProtected
				continue
			} else if victimQueue == queueProtected {
				victim = c.window.head
				victimQueue = queueWindow
				continue
			}

			// The pending operations will adjust the size to reflect the correct weight.
			break
		}

		// Skip over zero weight.
		if victim != nil && victim.weight() == 0 {
			victim = victim.next
			continue
		} else if candidate != nil && candidate.weight() == 0 {
			candidate = candidate.next
			continue
		}

		// Evict immediatly if only one is present after this point.
		if victim == nil {
			evict := candidate
			candidate = candidate.next
			c.evictEntry(evict)
			continue
		} else if candidate == nil {
			evict := victim
			victim = victim.next
			c.evictEntry(evict)
			continue
		}

		// Evict immediatly if they are both the same.
		if candidate == victim {
			victim = victim.next
			c.evictEntry(candidate)
			candidate = nil
			continue
		}

		if victim.dead.Load() {
			evict := victim
			victim = victim.next
			c.evictEntry(evict)
			continue
		} else if candidate.dead.Load() {
			evict := candidate
			candidate = candidate.next
			c.evictEntry(evict)
			continue
		}

		if c.admit(candidate.hash, victim.hash) {
			evict := victim
			victim = victim.next
			c.evictEntry(evict)
			continue
		} else {
			evict := candidate
			candidate = candidate.next
			c.evictEntry(evict)
			continue
		}
	}
}

func (c *cache) admit(candidateHash, victimHash uint64) bool {
	candidateFreq := c.frequencySketch.Frequency(candidateHash)
	victimFreq := c.frequencySketch.Frequency(victimHash)

	const AdmitHashDosThreshold = 6

	if candidateFreq > victimFreq {
		return true
	} else if candidateFreq >= AdmitHashDosThreshold {
		return fastrand()&127 == 0
	}
	return false
}

func (c *cache) evictEntry(e *entry) {
	if Debug {
		log.Printf("evicting %d", e.hash)
	}

	switch e.queue {
	case queueWindow:
		c.window.Remove(e)
		c.windowSize -= e.weight()
	case queueProbation:
		c.probation.Remove(e)
	case queueProtected:
		c.protected.Remove(e)
		c.protectedSize -= e.weight()
	}

	c.size -= e.weight()

	// Wait for any read to complete.
	e.bufLock.Lock()
	e.dead.Store(true)
	e.bufLock.Unlock()

	c.data.Delete(e.hash)
	c.pool.Put(e)
}

func (c *cache) onAccess(e *entry) {
	c.frequencySketch.Increment(e.hash)
	switch e.queue {
	case queueWindow:
		reorder(c.window, e)
	case queueProbation:
		c.reorderProbation(e)
	case queueProtected:
		reorder(c.protected, e)
	}
	c.hitsInSample += 1
}

func (c *cache) reorderProbation(e *entry) {
	if !c.probation.Contains(e) {
		return
	}

	if e.weight() > c.protectedCap {
		reorder(c.probation, e)
		return
	}

	c.protectedSize += e.weight()
	c.probation.Remove(e)
	c.protected.OfferFront(e)
	e.queue = queueProtected
}

func reorder(deque entryDeque, e *entry) {
	if deque.Contains(e) {
		deque.MoveBack(e)
	}
}

func (c *cache) afterRead(e *entry) {
	idx := int(fastrand() & uint32(readBufferSize-1))
	returned := c.readBuffers[idx].Add(e)
	if returned != nil {
		c.evictionLock.Lock()
		for _, entry := range returned {
			c.onAccess(entry)
		}
		c.evictionLock.Unlock()
		c.readBuffers[idx].Free()
	}
}

func (c *cache) Entries() int {
	return c.data.Size()
}

func (c *cache) WaitForIdle() {
	for !c.writeBuffer.Empty() {
		runtime.Gosched()
	}
}
