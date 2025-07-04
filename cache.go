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
	"encoding/gob"
	"fmt"
	"hash/maphash"
	"io"
	"math"
	"os"
	"runtime"
	"sync"
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

	// Metadata
	pool sync.Pool

	// Policy
	m, s *queue[*entry]
	g    *ghost

	// Storage
	alloc   *queue[int]
	hashmap *hmap[*entry]

	seed              maphash.Seed
	slotSize, slotCap int
	mCap, sCap        int
}

// New creates a new otto.Cache with a fixed size of slotSize * slotCount. Calling the
// Set method on a full cache will cause elements to be evicted according to the
// S3-FIFO algorithm.
func New(slotSize, slotCap int) Cache {
	mCap, sCap := defaultEx(slotCap)
	return NewEx(slotSize, mCap, sCap)
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

	c := &cache{
		data:     make([]byte, slotCap*slotSize),
		pool:     sync.Pool{New: newEntry},
		seed:     maphash.MakeSeed(),
		m:        newQueue[*entry](mCap),
		s:        newQueue[*entry](sCap),
		g:        newGhost(mCap),
		alloc:    newQueue[int](slotCap),
		hashmap:  newMap[*entry](slotCap),
		slotSize: slotSize,
		slotCap:  slotCap,
		mCap:     mCap,
		sCap:     sCap,
	}

	for i := range c.slotCap {
		if !c.alloc.TryEnqueue(i) {
			panic("otto: invariant violated: failed to enqueue on empty alloc queue")
		}
	}

	return c
}

func (c *cache) slice(i int) []byte {
	return c.data[i*c.slotSize : (i+1)*c.slotSize]
}

func (c *cache) Set(key string, val []byte) {
	if len(val) == 0 || c.closed.Load() {
		return
	}

	hash := maphash.String(c.seed, key)
	hash += c.hashOffset.Load()

	c.set(hash, val, frequencyMin)
}

const (
	frequencyMin = 0
	frequencyMax = 3
)

func (c *cache) set(hash uint64, val []byte, frequency int32) {
	if _, ok := c.hashmap.LoadOrStore(hash, nil); ok {
		// Already in the cache and we don't support updates
		return
	}

	size := len(val)
	slots := cost(c.slotSize, size)

	if slots > c.slotCap {
		// entry cannot fit in cache
		return
	}

	var (
		first  = -1
		prev   int
		offset int
	)

	for !c.alloc.TryDequeueBatch(slots, func(i int) {
		if first == -1 {
			first = i
		} else {
			binary.NativeEndian.PutUint64(c.slice(prev), uint64(i))
		}

		offset += copy(c.slice(i)[headerSize:], val[offset:])
		prev = i
	}) {
		c.evict()
	}

	e := c.pool.Get().(*entry)
	e.hash = hash
	e.size = size
	e.slot = first
	e.freq.Store(frequency)
	e.access.Store(0)

	c.size.Add(uint64(size))

	c.hashmap.Store(hash, e)

	if c.g.In(hash) {
		for !c.m.TryEnqueue(e) {
			c.evictM()
		}
		c.mSize.Add(int64(slots))
	} else {
		for !c.s.TryEnqueue(e) {
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
	hash += c.hashOffset.Load()

	return c.get(hash, dst)
}

func (c *cache) get(hash uint64, dst []byte) []byte {
	e, ok := c.hashmap.Load(hash)
	if !ok || e == nil {
		return nil
	}

	if e.access.Add(1) < 0 {
		e.access.Store(math.MinInt32)
		return nil
	}

	for {
		freq := e.freq.Load()
		if freq == frequencyMax {
			break
		}

		if e.freq.CompareAndSwap(freq, min(freq+1, frequencyMax)) {
			break
		}
	}

	dst = c.read(e, dst)

	e.access.Add(-1)

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
	e, ok := c.s.TryDequeue()
	for ok {
		slots := int64(cost(c.slotSize, e.size))

		c.sSize.Add(-slots)

		// Small variation from the S3-FIFO algorithm. If a value of
		// frequency 0 is concurrently accessed as the eviction runs
		// it will have a frequency of 1 and it will still get promoted
		// to the m-queue.
		if e.freq.Load() <= 1 && e.access.CompareAndSwap(0, math.MinInt32) {
			c.g.Add(e.hash)
			c.evictEntry(e)
			return
		}

		for !c.m.TryEnqueue(e) {
			c.evictM()
		}
		c.mSize.Add(slots)

		e, ok = c.s.TryDequeue()
	}
}

func (c *cache) evictM() {
	e, ok := c.m.TryDequeue()
	for ok {
		slots := int64(cost(c.slotSize, e.size))

		c.mSize.Add(-slots)

		if e.freq.Load() <= 0 && e.access.CompareAndSwap(0, math.MinInt32) {
			c.evictEntry(e)
			return
		}

		for {
			freq := e.freq.Load()

			if e.freq.CompareAndSwap(freq, max(0, freq-1)) {
				for !c.m.TryEnqueue(e) {
					c.evictM()
				}
				c.mSize.Add(slots)

				break
			}
		}

		e, ok = c.m.TryDequeue()
	}
}

func (c *cache) evictEntry(e *entry) {
	if prev, ok := c.hashmap.LoadAndDelete(e.hash); !ok || e != prev {
		panic("otto: invariant violated: entry already deleted")
	}

	if e.size < 1 {
		panic("otto: invariant violated: entry with size zero")
	}

	c.size.Add(^uint64(e.size - 1))

	slots := cost(c.slotSize, e.size)

	prev := e.slot
	c.pool.Put(e)

	var next, current int
	for !c.alloc.TryEnqueueBatch(slots, func() int {
		next = int(binary.NativeEndian.Uint64(c.slice(prev)))
		current = prev
		prev = next
		return current
	}) {
		runtime.Gosched()
	}

	for range slots {
	}
}

func (c *cache) read(e *entry, dst []byte) []byte {
	size := e.size
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}

	slots := cost(c.slotSize, size)

	i := e.slot

	var offset, next int
	for range slots {
		next = int(binary.NativeEndian.Uint64(c.slice(i)))
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
	_ = c.hashmap.Range(func(_ uint64, e *entry) error {
		e.freq.Store(0)
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

const serializeVersionHeader = "otto-cache-1.0.0"

type serializedEntry struct {
	Key   uint64
	Value []byte
	Freq  int32
}

func (c *cache) Serialize(w io.Writer) error {
	e := gob.NewEncoder(w)

	if err := e.Encode(serializeVersionHeader); err != nil {
		return fmt.Errorf("failed to encode version header: %w", err)
	}

	seed := *(*uint64)(unsafe.Pointer(&c.seed))
	if err := e.Encode(seed); err != nil {
		return fmt.Errorf("failed to encode seed: %w", err)
	}

	err := c.hashmap.Range(func(k uint64, v *entry) error {
		se := serializedEntry{
			Key:   k,
			Value: c.read(v, nil),
			Freq:  v.freq.Load(),
		}

		return e.Encode(se)
	})

	if err != nil {
		return fmt.Errorf("failed to encode entry: %w", err)
	}

	return err
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

	var versionHeader string
	if err := d.Decode(&versionHeader); err != nil {
		return nil, fmt.Errorf("failed to decode version header: %w", err)
	}

	if versionHeader != serializeVersionHeader {
		return nil, fmt.Errorf("unsupported version header: %s", versionHeader)
	}

	var rawSeed uint64
	if err := d.Decode(&rawSeed); err != nil {
		return nil, fmt.Errorf("failed to decode seed: %w", err)
	}

	seed := *(*maphash.Seed)(unsafe.Pointer(&rawSeed))

	c := NewEx(slotSize, mCap, sCap).(*cache)
	c.seed = seed

	for {
		var se serializedEntry
		if err := d.Decode(&se); err != nil {
			if err == io.EOF {
				break // End of stream
			}

			return nil, fmt.Errorf("failed to decode entry: %w", err)
		}

		clampedFreq := max(min(se.Freq, frequencyMax), frequencyMin)

		c.set(se.Key, se.Value, clampedFreq)
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

// LoadFromFileEx deserializes the cache from the file at the provided path.
// Refer to the NewEx method for the usage of slotSize & mCap & sCap arguments.
func LoadFromFileEx(path string, slotSize, mCap, sCap int) (Cache, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	cache, err := DeserializeEx(file, slotSize, mCap, sCap)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize from file: %w", err)
	}

	return cache, file.Close()
}
