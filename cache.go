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
	"encoding/binary"
	"errors"
	"hash/maphash"
	"io"
	"math"
	"runtime"
	"sync/atomic"
	"unsafe"
)

var ErrEmptyValue = errors.New("otto: can't add empty value to cache")
var ErrClosedCache = errors.New("otto: can't perform operation on closed cache")
var ErrAlreadyExists = errors.New("otto: key already exists in cache")
var ErrTooBig = errors.New("otto: value is too big for cache")

type Cache interface {
	// Set inserts an item in the cache. If the cache is full an element
	// will be evicted.
	//
	// NOTE: updates are not supported.
	Set(key string, val []byte) error

	// Get retrieves - if available - an item from the cache. If the item
	// does not exist it will return nil.
	Get(key string, buf []byte) []byte

	// Has checks if an item - identified by its key - is present in the
	// cache.
	Has(key string) bool

	// Clear clears the cache, removing every entry without freeing the memory.
	Clear()

	// Close clears the cache, removing every entry and freeing the memory.
	Close() error

	// Entries returns the number of entries currently stored in the cache.
	Entries() uint64

	// Size returns the sum of the sizes of all the entries currently
	// stored in the cache.
	Size() uint64

	// Capacity returns the theoretical capacity of the cache.
	Capacity() uint64

	// Serialize write the cache to a writer. The cache can then be
	// reinstantiated using the otto.Deserialize function.
	Serialize(w io.Writer) error
}

type InternalStatsCache interface {
	Cache

	// MQueueEntries returns the number of entries in the M-queue.
	MQueueEntries() uint64

	// MQueueCapacity returns the capacity of the M-queue.
	MQueueCapacity() uint64

	// SQueueEntries returns the number of entries in the S-queue.
	SQueueEntries() uint64

	// SQueueCapacity returns the capacity of the S-queue.
	SQueueCapacity() uint64
}

var _ (InternalStatsCache) = (*cache)(nil)

type cache struct {
	// Stats
	size atomic.Uint64
	//lint:ignore U1000 prevents false sharing
	_ [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte

	mSize atomic.Int64
	//lint:ignore U1000 prevents false sharing
	_ [cacheLineSize - unsafe.Sizeof(atomic.Int64{})]byte

	sSize atomic.Int64
	//lint:ignore U1000 prevents false sharing
	_ [cacheLineSize - unsafe.Sizeof(atomic.Int64{})]byte

	hashOffset atomic.Uint64
	//lint:ignore U1000 prevents false sharing
	_ [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte

	closed atomic.Bool
	//lint:ignore U1000 prevents false sharing
	_ [cacheLineSize - unsafe.Sizeof(atomic.Bool{})]byte

	// Memory slots
	data []byte

	// Entries
	eSize   []int32
	eAccess []atomic.Int32
	eFreq   []atomic.Int32
	eHash   []uint64

	// Policy
	m, s *queue[int32]
	g    *ghost

	// Storage
	alloc   *queue[int32]
	hashmap *hmap[int32]

	seed              maphash.Seed
	slotSize, slotCap int32
	mCap, sCap        int32
}

// New creates a new otto.Cache with a fixed size of slotSize * slotCount. Calling the
// Set method on a full cache will cause elements to be evicted according to the
// S3-FIFO algorithm.
func New(slotSize, slotCap int32) Cache {
	mCap, sCap := defaultEx(slotCap)
	return NewEx(slotSize, mCap, sCap)
}

const (
	DefaultSmallQueuePercent = 10
)

func defaultEx(slotCount int32) (mCapacity, sCapacity int32) {
	mCapacity = (slotCount * (100 - DefaultSmallQueuePercent)) / 100
	sCapacity = slotCount - mCapacity
	return mCapacity, sCapacity
}

// NewEx creates a new otto.Cache with a fixed size of slotSize * (mCap + sCap).
// Calling the Set method on a full cache will cause elements to be evicted according to
// the S3-FIFO algorithm.
//
// mCapacity and sCapacity configure the size of the m and the s queue respectively. To
// learn more about S3-FIFO visit https://s3fifo.com/
func NewEx(slotSize, mCap, sCap int32) Cache {
	slotCap := mCap + sCap

	c := &cache{
		data:     make([]byte, slotCap*slotSize),
		eAccess:  make([]atomic.Int32, slotCap),
		eFreq:    make([]atomic.Int32, slotCap),
		eSize:    make([]int32, slotCap),
		eHash:    make([]uint64, slotCap),
		seed:     maphash.MakeSeed(),
		m:        newQueue[int32](int(mCap)),
		s:        newQueue[int32](int(sCap)),
		g:        newGhost(int(mCap)),
		alloc:    newQueue[int32](int(slotCap)),
		hashmap:  newMap[int32](int(slotCap)),
		slotSize: slotSize,
		slotCap:  slotCap,
		mCap:     mCap,
		sCap:     sCap,
	}

	for i := range c.slotCap {
		if c.alloc.TryEnqueue(i) != enqueueOk {
			panic("otto: invariant violated: failed to enqueue on empty alloc queue")
		}
	}

	return c
}

func (c *cache) slice(i int32) []byte {
	return c.data[i*c.slotSize : (i+1)*c.slotSize]
}

func (c *cache) Set(key string, val []byte) error {
	if len(val) == 0 {

		return ErrEmptyValue
	}

	if c.closed.Load() {
		return ErrClosedCache
	}

	hash := maphash.String(c.seed, key)
	hash += c.hashOffset.Load()

	return c.set(hash, val, frequencyMin, false)
}

const (
	frequencyMin = 0
	frequencyMax = 3
	nilEntry     = -1
)

func (c *cache) set(hash uint64, val []byte, frequency int32, useMQueue bool) error {

	if _, ok := c.hashmap.LoadOrStore(hash, nilEntry); ok {
		// Already in the cache and we don't support updates
		return ErrAlreadyExists
	}

	size := int32(len(val))
	slots := cost(c.slotSize, size)

	if slots > c.slotCap {
		// entry cannot fit in cache
		return ErrTooBig
	}

	var (
		first   int32 = -1
		prev    int32
		offset  int
		dequeue dequeueResult
	)

	for try := true; try; try = (dequeue != dequeueOk) {
		dequeue = c.alloc.TryDequeueBatch(int(slots), func(i int32) {
			if first == -1 {
				first = i
			} else {
				binary.NativeEndian.PutUint32(c.slice(prev), uint32(i))
			}

			offset += copy(c.slice(i)[headerSize:], val[offset:])
			prev = i
		})

		if dequeue == dequeueEmpty {
			c.evict()
		}
	}

	c.eAccess[first].Store(0)
	c.eFreq[first].Store(frequency)
	c.eSize[first] = size
	c.eHash[first] = hash

	c.size.Add(uint64(size))

	c.hashmap.Store(hash, first)

	var enqueue enqueueResult

	if useMQueue || c.g.In(hash) {
		for try := true; try; try = (enqueue != enqueueOk) {
			enqueue = c.m.TryEnqueue(first)
			if enqueue == enqueueFull {
				c.evictM()
			}
		}
		c.mSize.Add(int64(slots))
	} else {
		for try := true; try; try = (enqueue != enqueueOk) {
			enqueue = c.s.TryEnqueue(first)
			if enqueue == enqueueFull {
				c.evictS()
			}
		}
		c.sSize.Add(int64(slots))
	}

	return nil
}

func (c *cache) Get(key string, dst []byte) []byte {
	if c.closed.Load() {
		return nil
	}

	hash := maphash.String(c.seed, key)
	hash += c.hashOffset.Load()

	return c.get(hash, dst)
}

func (c *cache) get(hash uint64, dst []byte) []byte {
	e, ok := c.hashmap.Load(hash)
	if !ok || e == nilEntry {
		return nil
	}

	if c.eAccess[e].Add(1) < 0 {
		c.eAccess[e].Store(math.MinInt32)
		return nil
	}

	for {
		freq := c.eFreq[e].Load()
		if freq == frequencyMax {
			break
		}

		if c.eFreq[e].CompareAndSwap(freq, min(freq+1, frequencyMax)) {
			break
		}
	}

	dst = c.read(e, dst)

	c.eAccess[e].Add(-1)

	return dst
}

func (c *cache) Has(key string) bool {
	if c.closed.Load() {
		return false
	}

	hash := maphash.String(c.seed, key)
	_, ok := c.hashmap.Load(hash)
	return ok
}

func (c *cache) evict() {
	if c.sSize.Load() >= int64(c.s.cap) {
		c.evictS()
	} else {
		c.evictM()
	}
}

func (c *cache) evictS() {
	var (
		entry   int32
		dequeue dequeueResult
	)

	for try := true; try; try = (dequeue != dequeueEmpty) {
		entry, dequeue = c.s.TryDequeue()
		if dequeue == dequeueOk {
			slots := int64(cost(c.slotSize, c.eSize[entry]))

			c.sSize.Add(-slots)

			// Small variation from the S3-FIFO algorithm. If a value of
			// frequency 0 is concurrently accessed as the eviction runs
			// it will have a frequency of 1 and it will still get promoted
			// to the m-queue.
			if c.eFreq[entry].Load() <= 1 && c.eAccess[entry].CompareAndSwap(0, math.MinInt32) {
				c.g.Add(c.eHash[entry])
				c.evictEntry(entry)
				return
			}

			var enqueue enqueueResult
			for tryEnqueue := true; tryEnqueue; tryEnqueue = (enqueue != enqueueOk) {
				enqueue = c.m.TryEnqueue(entry)
				if enqueue == enqueueFull {
					c.evictM()
				}
			}

			c.mSize.Add(slots)
		}
	}
}

func (c *cache) evictM() {
	var (
		entry   int32
		dequeue dequeueResult
	)

	for try := true; try; try = (dequeue != dequeueEmpty) {
		entry, dequeue = c.m.TryDequeue()
		if dequeue == dequeueOk {
			slots := int64(cost(c.slotSize, c.eSize[entry]))

			c.mSize.Add(-slots)

			if c.eFreq[entry].Load() <= 0 && c.eAccess[entry].CompareAndSwap(0, math.MinInt32) {
				c.evictEntry(entry)
				return
			}

			for {
				freq := c.eFreq[entry].Load()

				if c.eFreq[entry].CompareAndSwap(freq, max(0, freq-1)) {
					var enqueue enqueueResult
					for tryEnqueue := true; tryEnqueue; tryEnqueue = (enqueue != enqueueOk) {
						enqueue = c.m.TryEnqueue(entry)
						if enqueue == enqueueFull {
							c.evictM()
						}
					}

					c.mSize.Add(slots)

					break
				}
			}

		}
	}
}

func (c *cache) evictEntry(e int32) {
	if prev, ok := c.hashmap.LoadAndDelete(c.eHash[e]); !ok || e != prev {
		panic("otto: invariant violated: entry already deleted")
	}

	if c.eSize[e] < 1 {
		panic("otto: invariant violated: entry with size zero")
	}

	c.size.Add(^uint64(c.eSize[e] - 1))

	slots := cost(c.slotSize, c.eSize[e])

	prev := e

	var next, current int32
	for c.alloc.TryEnqueueBatch(int(slots), func() int32 {
		next = int32(binary.NativeEndian.Uint32(c.slice(prev)))
		current = prev
		prev = next
		return current
	}) != enqueueOk {
		runtime.Gosched()
	}

	for range slots {
	}
}

func (c *cache) read(e int32, dst []byte) []byte {
	size := c.eSize[e]
	if cap(dst) < int(size) {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}

	slots := cost(c.slotSize, size)

	i := e

	var (
		offset int
		next   int32
	)
	for range slots {
		next = int32(binary.NativeEndian.Uint32(c.slice(i)))
		offset += copy(
			dst[offset:],
			c.slice(i)[headerSize:],
		)
		i = next
	}

	return dst
}

func (c *cache) Clear() {
	c.hashOffset.Add(1)
	_ = c.hashmap.Range(func(_ uint64, e int32) error {
		c.eFreq[e].Store(0)
		return nil
	})
}

func (c *cache) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *cache) Entries() uint64 {
	return uint64(c.hashmap.Size())
}

func (c *cache) Size() uint64 {
	return c.size.Load()
}

func (c *cache) Capacity() uint64 {
	return uint64(c.slotCap * c.slotSize)
}

func (c *cache) MQueueCapacity() uint64 {
	return c.m.cap
}

func (c *cache) MQueueEntries() uint64 {
	return uint64(c.mSize.Load())
}

func (c *cache) SQueueCapacity() uint64 {
	return c.s.cap
}

func (c *cache) SQueueEntries() uint64 {
	return uint64(c.sSize.Load())
}
