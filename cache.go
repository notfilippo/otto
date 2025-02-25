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
	Set(key string, val []byte)
	Get(key string, buf []byte) []byte
	Clear()

	SaveToFile(path string) error
	Serialize(w io.Writer) error
}

type cache struct {
	alloc   *allocator
	m, s    *queue[*entry]
	g       *ghost
	hashmap *hmap
	seed    maphash.Seed

	slotSize, slotCount int
	mCap, sCap          int
	mSize, sSize        atomic.Int64
}

func New(slotSize, slotCount int) Cache {
	mCap := (slotCount * 90) / 100
	sCap := slotCount - mCap
	return &cache{
		alloc:     newAllocator(slotSize+entrySize, slotCount),
		m:         newQueue[*entry](mCap),
		s:         newQueue[*entry](sCap),
		g:         newGhost(mCap),
		hashmap:   newMap(withPresize(slotCount)),
		seed:      maphash.MakeSeed(),
		slotSize:  slotSize,
		slotCount: slotCount,
		mCap:      mCap,
		sCap:      sCap,
	}
}

func (c *cache) Set(key string, val []byte) {
	hash := maphash.String(c.seed, key)

	if Debug {
		fmt.Printf("adding key %s string (hash %d) size = %d cost = %d\n", key, hash, len(val), c.cost(len(val)))
	}

	c.set(hash, val)
}

func (c *cache) set(hash uint64, val []byte) {
	if val == nil {
		return	
	}
	
	if _, ok := c.hashmap.LoadOrStore(hash, nil); ok {
		// Already in the cache
		return
	}

	if len(val) == 0 {
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

	c.hashmap.Store(hash, first)

	if c.g.In(hash) {
		for !c.m.TryEnqueue(first) {
			c.evictM()
		}
		c.mSize.Add(int64(cost))
	} else {
		for !c.s.TryEnqueue(first) {
			c.evictS()
		}
		c.sSize.Add(int64(cost))
	}
}

func (c *cache) Get(key string, buf []byte) []byte {
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
	if c.sSize.Load() >= int64(c.sCap) {
		c.evictS()
	} else {
		c.evictM()
	}
}

func (c *cache) evictS() {
	t, ok := c.s.TryDequeue()
	for ok && t != nil {
		c.sSize.Add(-int64(c.cost(t.size)))

		if t.frequency.Load() <= 1 && t.access.CompareAndSwap(0, math.MinInt32) {
			c.g.Add(t.hash)
			c.evictEntry(t)
			return
		} else {
			for !c.m.TryEnqueue(t) {
				c.evictM()
			}
			c.mSize.Add(int64(c.cost(t.size)))
		}

		t, ok = c.s.TryDequeue()
	}
}

func (c *cache) evictM() {
	t, ok := c.m.TryDequeue()
	for ok && t != nil {
		c.mSize.Add(-int64(c.cost(t.size)))

		if t.frequency.Load() <= 0 && t.access.CompareAndSwap(0, math.MinInt32) {
			c.evictEntry(t)
			return
		} else {
			for {
				freq := t.frequency.Load()

				if t.frequency.CompareAndSwap(freq, max(0, freq-1)) {
					for !c.m.TryEnqueue(t) {
						c.evictM()
					}

					c.mSize.Add(int64(c.cost(t.size)))
					break
				}
			}
		}

		t, ok = c.m.TryDequeue()
	}
}

func (c *cache) evictEntry(e *entry) {
	if Debug {
		fmt.Printf("evicting key hash %d size = %d cost = %d\n", e.hash, e.size, c.cost(e.size))
	}

	c.hashmap.Delete(e.hash)

	if e.size < 1 {
		panic("otto: invariant violated: size")
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
		source := unsafe.Slice(slot, c.slotSize+entrySize)

		end := min((i+1)*c.slotSize, size)
		copy(buf[i*c.slotSize:end], source[entrySize:])

		slot = e.next
	}

	return buf
}

type entry struct {
	hash uint64
	next *byte
	size int

	frequency atomic.Int32
	access    atomic.Int32
}

var entrySize = int(unsafe.Sizeof(entry{}))

func (c *cache) Clear() {
	c.hashmap = newMap(withPresize(c.slotCount))
	c.alloc.Clear()
	c.m = newQueue[*entry](c.mCap)
	c.s = newQueue[*entry](c.sCap)
	c.g = newGhost(c.mCap)
	c.mSize.Store(0)
	c.sSize.Store(0)
}

func (c *cache) SaveToFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	if err := c.Serialize(file); err != nil {
		return fmt.Errorf("failed to serialize to file: %w", err)
	}

	return file.Close()
}

func (c *cache) Serialize(w io.Writer) error {
	e := gob.NewEncoder(w)

	if err := e.Encode(c.slotSize); err != nil {
		return err
	}

	if err := e.Encode(c.slotCount); err != nil {
		return err
	}

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

func LoadFromFile(path string) (Cache, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	cache, err := Deserialize(file)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize to file: %w", err)
	}

	return cache, file.Close()
}

func Deserialize(r io.Reader) (Cache, error) {
	d := gob.NewDecoder(r)
	var chunkSize, chunkCount int
	if err := d.Decode(&chunkSize); err != nil {
		return nil, err
	}

	if err := d.Decode(&chunkCount); err != nil {
		return nil, err
	}

	var rawSeed uint64
	if err := d.Decode(&rawSeed); err != nil {
		return nil, err
	}

	seed := *(*maphash.Seed)(unsafe.Pointer(&rawSeed))

	plain := make(map[uint64][]byte)
	if err := d.Decode(&plain); err != nil {
		return nil, err
	}

	c := New(chunkSize, chunkCount).(*cache)
	c.seed = seed

	for k, v := range plain {
		c.set(k, v)
	}

	return c, nil
}
