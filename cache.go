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

var (
	Debug = false
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

	entries atomic.Uint64
	//lint:ignore U1000 prevents false sharing
	epad [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte

	memoryUsed atomic.Int64
	//lint:ignore U1000 prevents false sharing
	mpad [cacheLineSize - unsafe.Sizeof(atomic.Int64{})]byte

	alloc   *allocator
	m, s    *entryQueue
	g       *ghost
	hashmap *hmap

	seed                maphash.Seed
	slotSize, slotCount int
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

// NewEx creates a new otto.Cache with a fixed size of slotSize * (mCapacity + sCapacity).
// Calling the Set method on a full cache will cause elements to be evicted according to
// the S3-FIFO algorithm.
//
// mCapacity and sCapacity configure the size of the m and the s queue respectively. To
// learn more about S3-FIFO visit https://s3fifo.com/
func NewEx(slotSize, mCapacity, sCapacity int) Cache {
	slotCount := mCapacity + sCapacity

	entryAlign := int(unsafe.Alignof(entry{}))

	fullSlotSize := entrySize + slotSize
	remainder := fullSlotSize % entryAlign
	if remainder != 0 {
		// We need to round up to ensure alignment.
		fullSlotSize = fullSlotSize + (entryAlign - remainder)
	}

	slotSize = fullSlotSize - entrySize

	return &cache{
		alloc:     newAllocator(fullSlotSize, slotCount),
		m:         newEntryQueue(mCapacity),
		s:         newEntryQueue(sCapacity),
		g:         newGhost(mCapacity),
		hashmap:   newMap(withPresize(slotCount)),
		seed:      maphash.MakeSeed(),
		slotSize:  slotSize,
		slotCount: slotCount,
	}
}

func (c *cache) Set(key string, val []byte) {
	if len(val) == 0 || c.closed.Load() {
		return
	}

	hash := maphash.String(c.seed, key)

	if Debug {
		fmt.Printf("adding key %s string (hash %d) size = %d cost = %d\n", key, hash, len(val), c.cost(len(val)))
	}

	c.set(hash, val)
}

func (c *cache) set(hash uint64, val []byte) {
	if _, ok := c.hashmap.LoadOrStore(hash, nil); ok {
		// Already in the cache and we don't support updates
		return
	}

	size := len(val)
	cost := c.cost(size)

	if cost > c.slotCount {
		panic(fmt.Sprintf("otto: no more memory to store key %d size = %d cost = %d", hash, size, cost))
	}

	var (
		prev  *entry
		first *entry
		i     int
	)

	for !c.alloc.Alloc(cost, func(b *byte) {
		e := (*entry)(unsafe.Pointer(b))
		if prev != nil {
			prev.next = b
		} else {
			first = e
		}

		e.hash = hash
		e.next = nil
		e.size = size
		e.frequency.Store(0)
		e.access.Store(0)

		buf := unsafe.Slice(b, entrySize+c.slotSize)
		copy(buf[entrySize:], val[i*c.slotSize:])

		prev = e
		i += 1
	}) {
		c.evict()
	}

	c.memoryUsed.Add(int64(size))

	c.hashmap.Store(hash, first)
	c.entries.Add(1)

	if c.g.In(hash) {
		for !c.m.Push(first) {
			c.evictM()
		}
	} else {
		for !c.s.Push(first) {
			c.evictS()
		}
	}
}

func (c *cache) Get(key string, buf []byte) []byte {
	if c.closed.Load() {
		return nil
	}

	hash := maphash.String(c.seed, key)

	if Debug {
		fmt.Printf("getting key %s string (hash %d)\n", key, hash)
	}

	return c.get(hash, buf)
}

func (c *cache) get(hash uint64, buf []byte) []byte {
	e, ok := c.hashmap.Load(hash)
	if !ok || e == nil {
		return nil
	}

	for {
		access := e.access.Load()
		if access < 0 {
			return nil
		}

		if e.access.CompareAndSwap(access, access+1) {
			break
		}
	}

	for {
		freq := e.frequency.Load()

		if e.frequency.CompareAndSwap(freq, min(freq+1, 3)) {
			break
		}
	}

	buf = c.read(e, buf)

	e.access.Add(-1)

	return buf
}

func (c *cache) evict() {
	if c.s.IsFull() {
		c.evictS()
	} else {
		c.evictM()
	}
}

func (c *cache) evictS() {
	t, ok := c.s.Pop()
	for ok && t != nil {
		if t.frequency.Load() <= 1 && t.access.CompareAndSwap(0, math.MinInt32) {
			c.g.Add(t.hash)
			c.evictEntry(t)
			return
		}

		for !c.m.Push(t) {
			c.evictM()
		}
		for c.m.IsFull() {
			c.evictM()
		}

		t, ok = c.s.Pop()
	}
}

func (c *cache) evictM() {
	t, ok := c.m.Pop()
	for ok && t != nil {
		if t.frequency.Load() <= 0 && t.access.CompareAndSwap(0, math.MinInt32) {
			c.evictEntry(t)
			return
		}

		for {
			freq := t.frequency.Load()

			if t.frequency.CompareAndSwap(freq, max(0, freq-1)) {
				for !c.m.Push(t) {
					c.evictM()
				}

				break
			}
		}

		t, ok = c.m.Pop()
	}
}

func (c *cache) evictEntry(e *entry) {
	if Debug {
		fmt.Printf("evicting key hash %d size = %d cost = %d\n", e.hash, e.size, c.cost(e.size))
	}

	if prev, ok := c.hashmap.LoadAndDelete(e.hash); !ok || prev != e {
		panic("otto: invariant violated: entry already deleted")
	}

	c.entries.Add(^uint64(0))
	c.memoryUsed.Add(-int64(e.size))

	if e.size < 1 {
		panic("otto: invariant violated: entry with size zero")
	}

	chunk := (*byte)(unsafe.Pointer(e))
	for range c.cost(e.size) {
		e := (*entry)(unsafe.Pointer(chunk))
		next := e.next
		for !c.alloc.Free(chunk) {
			runtime.Gosched()
		}
		chunk = next
	}
}

func (c *cache) cost(size int) int {
	return (size + c.slotSize - 1) / c.slotSize
}

func (c *cache) read(e *entry, buf []byte) []byte {
	size := e.size
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	slots := c.cost(size)

	slot := (*byte)(unsafe.Pointer(e))
	for i := range slots {
		e := (*entry)(unsafe.Pointer(slot))
		if slot == nil {
			// Corruption happened, returning nil
			return nil
		}

		source := unsafe.Slice(slot, c.slotSize+entrySize)

		end := min((i+1)*c.slotSize, size)
		copy(buf[i*c.slotSize:end], source[entrySize:])

		slot = e.next
	}

	return buf
}

func (c *cache) Clear() {
	mCap := (c.slotCount * 90) / 100
	sCap := c.slotCount - mCap
	c.hashmap = newMap(withPresize(c.slotCount))
	c.alloc.Clear()
	c.m = newEntryQueue(mCap)
	c.s = newEntryQueue(sCap)
	c.g = newGhost(mCap)
	c.entries.Store(0)
}

func (c *cache) Close() error {
	c.closed.Store(true)
	return c.alloc.Close()
}

func (c *cache) Entries() uint64 {
	return c.entries.Load()
}

func (c *cache) Size() uint64 {
	return uint64(c.memoryUsed.Load())
}

func (c *cache) Capacity() uint64 {
	return uint64(c.slotCount * c.slotSize)
}

func (c *cache) MQueueCapacity() uint64 {
	return c.m.fifo.cap
}

func (c *cache) MQueueEntries() uint64 {
	return uint64(c.m.size.Load())
}

func (c *cache) SQueueCapacity() uint64 {
	return c.s.fifo.cap
}

func (c *cache) SQueueEntries() uint64 {
	return uint64(c.s.size.Load())
}

func (c *cache) Serialize(w io.Writer) error {
	e := gob.NewEncoder(w)

	seed := *(*uint64)(unsafe.Pointer(&c.seed))
	if err := e.Encode(seed); err != nil {
		return err
	}

	plain := make(map[uint64][]byte)
	c.hashmap.Range(func(k uint64, v *entry) bool {
		plain[k] = c.get(k, nil)
		return true
	})

	return e.Encode(plain)
}

func Deserialize(r io.Reader, slotSize, slotCount int) (Cache, error) {
	mCapacity, sCapacity := defaultEx(slotCount)
	return DeserializeEx(r, slotSize, mCapacity, sCapacity)
}

func DeserializeEx(r io.Reader, slotSize, mCapacity, sCapacity int) (Cache, error) {
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

	c := NewEx(slotSize, mCapacity, sCapacity).(*cache)
	c.seed = seed

	for k, v := range plain {
		if len(v) == 0 {
			continue
		}

		c.set(k, v)
	}

	return c, nil
}

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

func LoadFromFile(path string, slotSize, slotCount int) (Cache, error) {
	mCapacity, sCapacity := defaultEx(slotCount)
	return LoadFromFileEx(path, slotSize, mCapacity, sCapacity)
}

func LoadFromFileEx(path string, slotSize, mCapacity, sCapacity int) (Cache, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	cache, err := DeserializeEx(file, slotSize, mCapacity, sCapacity)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize to file: %w", err)
	}

	return cache, file.Close()
}
