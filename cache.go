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
	"encoding/gob"
	"fmt"
	"hash/maphash"
	"io"
	"math"
	"os"
	"runtime"
	"sync/atomic"
	"unsafe"
)

type Cache interface {
	// Set inserts an item in the cache. If the cache is full an element
	// will be evicted.
	//
	// NOTE: updates are not supported.
	Set(key string, val []byte)

	// Get retrieves - if available - an item from the cache. If the item
	// does not exist it will return nil.
	Get(key string, buf []byte) []byte

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
	closed atomic.Bool
	//lint:ignore U1000 prevents false sharing
	cpad [cacheLineSize - unsafe.Sizeof(atomic.Bool{})]byte

	mSize atomic.Int64
	//lint:ignore U1000 prevents false sharing
	mpad [cacheLineSize - unsafe.Sizeof(atomic.Bool{})]byte

	sSize atomic.Int64
	//lint:ignore U1000 prevents false sharing
	spad [cacheLineSize - unsafe.Sizeof(atomic.Bool{})]byte

	data []byte

	entries []entry

	m, s *queue[int]
	g    *ghost

	alloc   *queue[int]
	hashmap *hmap[int]

	seed              maphash.Seed
	slotSize, slotCap int

	// Stats
	entriesCount atomic.Uint64
	memoryUsed   atomic.Int64
}

// New creates a new otto.Cache with a fixed size of slotSize * slotCount. Calling the
// Set method on a full cache will cause elements to be evicted according to the
// S3-FIFO algorithm.
func New(slotSize, slotCount int) Cache {
	mCapacity, sCapacity := defaultEx(slotCount)
	return NewEx(slotSize, mCapacity, sCapacity)
}

const (
	DefaultSmallQueuePercent = 10
)

func defaultEx(slotCount int) (mCapacity, sCapacity int) {
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
func NewEx(slotSize, mCap, sCap int) Cache {
	slotCap := mCap + sCap
	alloc := newQueue[int](slotCap)
	for i := range slotCap {
		if !alloc.TryEnqueue(i) {
			panic("otto: invariant violated: failed to enqueue on empty alloc queue")
		}
	}

	return &cache{
		data:     make([]byte, slotCap*slotSize),
		entries:  make([]entry, slotCap),
		m:        newQueue[int](mCap),
		s:        newQueue[int](sCap),
		g:        newGhost(mCap),
		alloc:    alloc,
		hashmap:  newMap[int](slotCap),
		seed:     maphash.MakeSeed(),
		slotSize: slotSize,
		slotCap:  slotCap,
	}
}

func (c *cache) slice(i int) []byte {
	return c.data[i*c.slotSize : (i+1)*c.slotSize]
}

func (c *cache) Set(key string, val []byte) {
	if len(val) == 0 || c.closed.Load() {
		return
	}

	hash := maphash.String(c.seed, key)

	c.set(hash, val)
}

func (c *cache) set(hash uint64, val []byte) {
	if _, ok := c.hashmap.LoadOrStore(hash, -1); ok {
		// Already in the cache and we don't support updates
		return
	}

	size := len(val)
	slots := cost(c.slotSize, size)

	if slots > c.slotCap {
		panic(fmt.Sprintf("otto: entry %d cannot fit in cache size = %d slots = %d", hash, size, slots))
	}

	var (
		first  int = -1
		prev   int
		offset int
	)

	for !c.alloc.TryDequeueBatch(slots, func(i int) {
		if first == -1 {
			first = i
		} else {
			c.entries[prev].next.Store(int64(i))
		}

		offset += copy(c.slice(i), val[offset:])
		prev = i
	}) {
		c.evict()
	}

	e := &c.entries[first]
	e.hash = hash
	e.size = size
	e.frequency.Store(0)
	e.access.Store(0)

	c.memoryUsed.Add(int64(size))

	c.hashmap.Store(hash, first)
	c.entriesCount.Add(1)

	if c.g.In(hash) {
		for !c.m.TryEnqueue(first) {
			c.evictM()
		}
		c.mSize.Add(int64(slots))
	} else {
		for !c.s.TryEnqueue(first) {
			c.evictS()
		}
		c.sSize.Add(int64(slots))
	}
}

func (c *cache) Get(key string, dst []byte) []byte {
	if c.closed.Load() {
		return nil
	}

	hash := maphash.String(c.seed, key)

	return c.get(hash, dst)
}

func (c *cache) get(hash uint64, dst []byte) []byte {
	i, ok := c.hashmap.Load(hash)
	if !ok {
		return nil
	}

	e := &c.entries[i]

	if e.access.Add(1) < 0 {
		e.access.Store(math.MinInt32)
		return nil
	}

	for {
		freq := e.frequency.Load()
		if freq == 3 {
			break
		}

		if e.frequency.CompareAndSwap(freq, min(freq+1, 3)) {
			break
		}
	}

	dst = c.read(i, e, dst)

	e.access.Add(-1)

	return dst
}

func (c *cache) evict() {
	if c.sSize.Load() >= int64(c.s.cap) {
		c.evictS()
	} else {
		c.evictM()
	}
}

func (c *cache) evictS() {
	i, ok := c.s.TryDequeue()
	for ok {
		e := &c.entries[i]
		slots := int64(cost(c.slotSize, e.size))

		c.sSize.Add(-slots)

		// Small variation from the S3-FIFO algorithm. If a value of
		// frequency 0 is concurrently accessed as the eviction runs
		// it will have a frequency of 1 and it will still get promoted
		// to the m-queue.
		if e.frequency.Load() <= 1 && e.access.CompareAndSwap(0, math.MinInt32) {
			c.g.Add(e.hash)
			c.evictEntry(i, e)
			return
		}

		for !c.m.TryEnqueue(i) {
			c.evictM()
		}
		c.mSize.Add(slots)

		i, ok = c.s.TryDequeue()
	}
}

func (c *cache) evictM() {
	i, ok := c.m.TryDequeue()
	for ok {
		e := &c.entries[i]
		slots := int64(cost(c.slotSize, e.size))

		c.mSize.Add(-slots)

		if e.frequency.Load() <= 0 && e.access.CompareAndSwap(0, math.MinInt32) {
			c.evictEntry(i, e)
			return
		}

		for {
			freq := e.frequency.Load()

			if e.frequency.CompareAndSwap(freq, max(0, freq-1)) {
				for !c.m.TryEnqueue(i) {
					c.evictM()
				}
				c.mSize.Add(slots)

				break
			}
		}

		i, ok = c.m.TryDequeue()
	}
}

func (c *cache) evictEntry(i int, e *entry) {
	if prev, ok := c.hashmap.LoadAndDelete(e.hash); !ok || prev != i {
		panic("otto: invariant violated: entry already deleted")
	}

	if e.size < 1 {
		panic("otto: invariant violated: entry with size zero")
	}

	slots := cost(c.slotSize, e.size)

	c.entriesCount.Add(^uint64(0))
	c.memoryUsed.Add(-int64(e.size))

	var next int
	for range slots {
		next = int(e.next.Load())
		e = &c.entries[next]
		for !c.alloc.TryEnqueue(i) {
			runtime.Gosched()
		}
		i = next
	}
}

func (c *cache) read(i int, e *entry, dst []byte) []byte {
	size := e.size
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}

	slots := cost(c.slotSize, size)

	var offset, next int
	for range slots {
		next = int(e.next.Load())
		e = &c.entries[next]
		offset += copy(
			dst[offset:],
			c.slice(i),
		)
		i = next
	}

	return dst
}

func (c *cache) Clear() {
	c.m = newQueue[int](int(c.m.cap))
	c.s = newQueue[int](int(c.s.cap))
	c.g = newGhost(c.g.cap)
	c.alloc = newQueue[int](c.slotCap)
	c.hashmap = newMap[int](c.slotCap)

	for i := range c.slotCap {
		if !c.alloc.TryEnqueue(i) {
			panic("otto: invariant violated: failed to enqueue on empty alloc queue")
		}
	}
}

func (c *cache) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *cache) Entries() uint64 {
	return c.entriesCount.Load()
}

func (c *cache) Size() uint64 {
	return uint64(c.memoryUsed.Load())
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

func (c *cache) Serialize(w io.Writer) error {
	e := gob.NewEncoder(w)

	seed := *(*uint64)(unsafe.Pointer(&c.seed))
	if err := e.Encode(seed); err != nil {
		return err
	}

	plain := make(map[uint64][]byte)
	c.hashmap.Range(func(k uint64, v int) bool {
		plain[k] = c.get(k, nil)
		return true
	})

	return e.Encode(plain)
}

// Deserialize deserializes the cache from a byte stream.
// Refer to the New method for the usage of slotSize & slotCount arguments.
func Deserialize(r io.Reader, slotSize, slotCount int) (Cache, error) {
	mCapacity, sCapacity := defaultEx(slotCount)
	return DeserializeEx(r, slotSize, mCapacity, sCapacity)
}

// DeserializeEx deserializes the cache from a byte stream.
// Refer to the NewEx method for the usage of slotSize & mCap & sCap arguments.
func DeserializeEx(r io.Reader, slotSize, mCap, sCap int) (Cache, error) {
	d := gob.NewDecoder(r)

	var rawSeed uint64
	if err := d.Decode(&rawSeed); err != nil {
		return nil, err
	}

	seed := *(*maphash.Seed)(unsafe.Pointer(&rawSeed))

	plain := make(map[uint64][]byte)
	if err := d.Decode(&plain); err != nil {
		return nil, err
	}

	c := NewEx(slotSize, mCap, sCap).(*cache)
	c.seed = seed

	for k, v := range plain {
		if len(v) == 0 {
			continue
		}

		c.set(k, v)
	}

	return c, nil
}

// SaveToFile serializes the cache as a file in the provided path.
func SaveToFile(c Cache, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	if err := c.Serialize(file); err != nil {
		return fmt.Errorf("failed to serialize to file: %w", err)
	}

	return file.Close()
}

// LoadFromFile deserializes the cache from the file at the provided path.
// Refer to the New method for the usage of slotSize & slotCount arguments.
func LoadFromFile(path string, slotSize, slotCount int) (Cache, error) {
	mCap, sCap := defaultEx(slotCount)
	return LoadFromFileEx(path, slotSize, mCap, sCap)
}

// LoadFromFile deserializes the cache from the file at the provided path.
// Refer to the NewEx method for the usage of slotSize & mCap & sCap arguments.
func LoadFromFileEx(path string, slotSize, mCap, sCap int) (Cache, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	cache, err := DeserializeEx(file, slotSize, mCap, sCap)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize to file: %w", err)
	}

	return cache, file.Close()
}
