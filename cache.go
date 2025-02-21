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
	"runtime/debug"
	"slices"
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

	chunkSize, chunkCount int
	mSize, sSize          int
}

func New(chunkSize, chunkCount int) Cache {
	mSize := (chunkCount * 90) / 100
	sSize := chunkCount - mSize
	return &cache{
		alloc:      newAllocator(chunkSize+entrySize, chunkCount),
		m:          newQueue[*entry](),
		s:          newQueue[*entry](),
		g:          newGhost(mSize),
		hashmap:    newMap(withPresize(chunkCount)),
		seed:       maphash.MakeSeed(),
		chunkSize:  chunkSize,
		chunkCount: chunkCount,
		mSize:      mSize,
		sSize:      sSize,
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
	if _, ok := c.hashmap.LoadOrStore(hash, nil); ok {
		// Already in the cache
		return
	}

	cost := c.cost(len(val))

	if cost > c.mSize+c.sSize {
		panic(fmt.Sprintf("otto: no more memory to store key %d size = %d cost = %d", hash, len(val), cost))
	}

	var (
		prev  *entry
		first *entry
	)

	for buf := range slices.Chunk(val, c.chunkSize) {
		chunk := c.alloc.Get()
		for chunk == nil || c.m.Size()+c.s.Size()+1 > c.chunkCount {
			c.evict()
			chunk = c.alloc.Get()
		}

		e := (*entry)(unsafe.Pointer(chunk))
		e.hash = hash
		e.size = len(val)
		e.frequency.Store(0)
		e.access.Store(0)
		e.next = nil

		if prev == nil {
			first = e
		} else {
			prev.next = chunk
		}

		chunkBytes := unsafe.Slice(chunk, c.chunkSize+entrySize)
		copy(chunkBytes[entrySize:], buf)

		prev = e
	}

	if c.g.In(hash) {
		c.m.Push(first, cost)
	} else {
		c.s.Push(first, cost)
	}

	c.hashmap.Store(hash, first)
}

func (c *cache) Get(key string, buf []byte) []byte {
	hash := maphash.String(c.seed, key)

	if Debug {
		fmt.Printf("getting key %s string (hash %d)\n", key, hash)
	}

	return c.get(hash, buf)
}

func (c *cache) get(hash uint64, buf []byte) []byte {
	e, _ := c.hashmap.Load(hash)
	if e == nil {
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

func (c *cache) Clear() {
	c.hashmap.Clear()
	c.alloc.Clear()
	c.m = newQueue[*entry]()
	c.s = newQueue[*entry]()
	c.g = newGhost(c.mSize)
}

func (c *cache) SaveToFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	return c.Serialize(file)
}

func (c *cache) Serialize(w io.Writer) error {
	e := gob.NewEncoder(w)

	if err := e.Encode(c.chunkSize); err != nil {
		return err
	}

	if err := e.Encode(c.chunkCount); err != nil {
		return err
	}

	seed := *(*uint64)(unsafe.Pointer(&c.seed))
	if err := e.Encode(seed); err != nil {
		return err
	}

	hashmap := toPlainMap(c.hashmap)
	plain := make(map[uint64][]byte)
	for k := range hashmap {
		plain[k] = c.get(k, nil)
	}

	return e.Encode(plain)
}

func LoadFromFile(path string) (Cache, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return Deserialize(file)
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

func (c *cache) evict() {
	if c.s.Size() > c.sSize {
		c.evictS()
	} else {
		c.evictM()
	}
}

func (c *cache) evictS() {
	for c.s.Size() > 0 {
		t := c.s.Pop()
		if t == nil {
			os.Exit(1)
			return
		} else if t.frequency.Load() <= 1 && t.access.CompareAndSwap(0, math.MinInt32) {
			c.g.Add(t.hash)
			c.evictEntry(t)
			return
		} else {
			c.m.Push(t, c.cost(t.size))
			if c.m.Size() > c.mSize {
				c.evictM()
			}
		}
	}
}

func (c *cache) evictM() {
	for c.m.Size() > 0 {
		t := c.m.Pop()
		if t == nil {
			return
		} else if t.frequency.Load() <= 0 && t.access.CompareAndSwap(0, math.MinInt32) {
			c.evictEntry(t)
			return
		} else {
			// Try to decrease to max(0, freq-1) or break and continue if deleted
			for {
				freq := t.frequency.Load()

				if t.frequency.CompareAndSwap(freq, max(0, freq-1)) {
					c.m.Push(t, c.cost(t.size))
					break
				}
			}
		}
	}
}

func (c *cache) evictEntry(node *entry) {
	c.hashmap.Delete(node.hash)

	chunk := (*byte)(unsafe.Pointer(node))
	for chunk != nil {
		e := (*entry)(unsafe.Pointer(chunk))
		next := e.next
		c.alloc.Put(chunk)
		chunk = next
	}

	if Debug {
		fmt.Printf("evicting key hash %d size = %d cost = %d\n", node.hash, node.size, c.cost(node.size))
	}
}

func (c *cache) cost(size int) int {
	return (size + c.chunkSize - 1) / c.chunkSize
}

func (c *cache) read(e *entry, buf []byte) []byte {
	size := e.size
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	chunksToScan := c.cost(size)

	defer func() {
		if err := recover(); err != nil {
			panic(fmt.Sprintf("corruption panic: %v: metadata: size = %d, chunksToScan = %d, len(buf) = %d, entry.access = %d, entry.frequency = %d, entry.size = %d\n%s", err, size, chunksToScan, len(buf), e.access.Load(), e.frequency.Load(), e.size, string(debug.Stack())))
		}	
	}()

	
	chunk := (*byte)(unsafe.Pointer(e))
	for i := 0; i < chunksToScan && chunk != nil; i++ {
		e := (*entry)(unsafe.Pointer(chunk))
		source := unsafe.Slice(chunk, c.chunkSize+entrySize)

		end := min((i+1)*c.chunkSize, size)
		copy(buf[i*c.chunkSize:end], source[entrySize:])

		i += 1
		chunk = e.next
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
