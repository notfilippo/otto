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
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
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

	// Capacity returns the theoretical capacity of the cache.
	Capacity() uint64

	// Serialize write the cache to a writer. The cache can then be
	// reinstantiated using the otto.Deserialize function.
	Serialize(w io.Writer) error
}

type cache struct {
	seed       maphash.Seed
	shards     []shard
	shardMask  uint32
	shardSize  uint32
	shardCount uint32
}

// New creates a new cache. The cache will immediately allocate
// shardSize * shardCount bytes of memory.
func New(shardSize, shardCount uint32) Cache {
	if shardCount != nextPowOf2(shardCount) {
		panic("shard size must be a power of 2")
	}

	arenaSize := int(shardSize) * int(shardCount)

	arena, err := unix.Mmap(-1, 0, arenaSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		panic(fmt.Errorf("cannot allocate %d shards of %d size = %d bytes via mmap: %w", shardCount, shardSize, arenaSize, err))
	}

	shards := make([]shard, 0, shardCount)
	for i := range shardCount {
		memory := arena[i*shardSize : (i+1)*shardSize]
		shards = append(shards, newShard(memory))
	}

	return &cache{
		seed:       maphash.MakeSeed(),
		shards:     shards,
		shardMask:  shardCount - 1,
		shardSize:  shardSize,
		shardCount: shardCount,
	}
}

func (c *cache) Set(key string, val []byte) {
	if len(val) == 0 {
		// Value of len 0 can be skipped as the library returns a
		// zero length slice if the value is not found.
		return
	}

	hash := maphash.String(c.seed, key)
	c.set(hash, val)
}

func (c *cache) set(hash uint64, val []byte) {
	shard := &c.shards[uint32(hash)&c.shardMask]

	size := uint32(headerSize + len(val))
	if size > c.shardSize {
		// Value is too big to fit in a shard.
		return
	}

	shard.mu.Lock()

	if _, ok := shard.entries[hash]; ok {
		// Value is already stored in the cache.
		shard.mu.Unlock()
		return
	}

	if shard.hand+size > c.shardSize {
		shard.fold += 1
		shard.hand = 0

		for k, e := range shard.entries {
			fold := uint32(e)
			if fold != shard.fold && fold != shard.fold-1 {
				delete(shard.entries, k)
			}
		}
	}

	header := (*header)(unsafe.Pointer(&shard.memory[shard.hand]))
	header.hash = hash
	header.size = uint32(len(val))
	copy(shard.memory[shard.hand+headerOffset:], val)

	e := uint64(shard.hand)<<32 | uint64(shard.fold)
	shard.entries[hash] = e
	shard.hand += size

	shard.mu.Unlock()
}

func (c *cache) Get(key string, buf []byte) []byte {
	hash := maphash.String(c.seed, key)
	return c.get(hash, buf)
}

func (c *cache) get(hash uint64, buf []byte) []byte {
	shard := &c.shards[uint32(hash)&c.shardMask]

	tok := shard.mu.RLock()

	e, ok := shard.entries[hash]
	index := uint32(e >> 32)
	fold := uint32(e)

	if !ok || (index >= shard.hand && fold != shard.fold-1) || (index < shard.hand && fold != shard.fold) {
		// Entry belongs to older fold.
		shard.mu.RUnlock(tok)
		return nil
	}

	header := (*header)(unsafe.Pointer(&shard.memory[index]))
	if header.hash != hash {
		// Corruption / collision.
		shard.mu.RUnlock(tok)
		return nil
	}

	if uint32(cap(buf)) < header.size {
		buf = make([]byte, header.size)
	} else {
		buf = buf[:header.size]
	}

	copy(buf, shard.memory[index+headerOffset:])
	shard.mu.RUnlock(tok)
	return buf
}

func (c *cache) Clear() {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		shard.hand = 0
		shard.fold = 0
		shard.entries = make(map[uint64]uint64)
		shard.mu.Unlock()
	}
}

func (c *cache) Close() error {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		shard.entries = nil
	}

	size := len(c.shards) * len(c.shards[0].memory)
	return unix.Munmap(c.shards[0].memory[0:size])
}

func (c *cache) Entries() uint64 {
	var total uint64

	for i := range c.shards {
		shard := &c.shards[i]
		tok := shard.mu.RLock()
		entries := len(shard.entries)
		shard.mu.RUnlock(tok)
		total += uint64(entries)
	}

	return total
}

func (c *cache) Capacity() uint64 {
	return uint64(c.shardCount) * uint64(c.shardSize)
}

func (c *cache) Serialize(w io.Writer) error {
	e := gob.NewEncoder(w)

	seed := *(*uint64)(unsafe.Pointer(&c.seed))
	if err := e.Encode(seed); err != nil {
		return err
	}

	plain := make(map[uint64][]byte)
	for i := range c.shards {
		shard := &c.shards[i]
		tok := shard.mu.RLock()
		for k := range shard.entries {
			plain[k] = c.get(k, nil)
		}
		shard.mu.RUnlock(tok)
	}

	return e.Encode(plain)
}

func Deserialize(r io.Reader, slotSize, slotCount uint32) (Cache, error) {
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

	c := New(slotSize, slotCount).(*cache)
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

func LoadFromFile(path string, slotSize, slotCount uint32) (Cache, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	cache, err := Deserialize(file, slotSize, slotCount)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize to file: %w", err)
	}

	return cache, file.Close()
}

type shard struct {
	mu      *rbMutex
	entries map[uint64]uint64
	hand    uint32
	fold    uint32
	memory  []byte
}

func newShard(memory []byte) shard {
	return shard{
		mu:      newRBMutex(),
		entries: make(map[uint64]uint64),
		memory:  memory,
	}
}

type header struct {
	hash uint64
	size uint32
}

var (
	headerSize   = int(unsafe.Sizeof(header{}))
	headerOffset = uint32(headerSize)
)
