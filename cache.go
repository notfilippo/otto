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
	"fmt"
	"hash/maphash"
	"io"
	"unsafe"

	"golang.org/x/sys/unix"
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

	// Serialize write the cache to a writer. The cache can then be
	// reinstantiated using the otto.Deserialize function.
	Serialize(w io.Writer) error
}

type cache struct {
	seed   maphash.Seed
	shards []shard
}

func New(shardCount, shardSize int) Cache {
	arenaSize := shardCount * shardSize
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
		seed:   maphash.MakeSeed(),
		shards: shards,
	}
}

func (c *cache) Set(key string, val []byte) {
	if len(val) == 0 {
		// Value of len 0 can be skipped as the library returns a
		// zero length slice if the value is not found.
		return
	}

	hash := maphash.String(c.seed, key)
	index := int(hash % uint64(len(c.shards)))
	shard := &c.shards[index]

	size := headerSize + len(val)

	if size > len(shard.memory) {
		// Value is too big to fit in a shard.
		return
	}

	shard.mu.Lock()

	if _, ok := shard.entries[hash]; ok {
		shard.mu.Unlock()
		return
	}

	var compact bool

	if shard.hand+size > len(shard.memory) {
		shard.gen = (shard.gen + 1) % 10
		shard.hand = 0
		compact = true
	}

	header := (*header)(unsafe.Pointer(&shard.memory[shard.hand]))
	header.hash = hash
	header.size = len(val)
	copy(shard.memory[shard.hand+headerSize:], val)

	shard.entries[hash] = entry{index: shard.hand, gen: shard.gen}

	shard.hand += size

	if compact {
		entries := make(map[uint64]entry)
		for i := range shard.entries {
			if shard.entries[i].gen == shard.gen || shard.entries[i].gen == shard.gen-1 {
				entries[i] = shard.entries[i]
			}
		}
		shard.entries = entries
	}

	shard.mu.Unlock()
}

func (c *cache) Get(key string, buf []byte) []byte {
	hash := maphash.String(c.seed, key)
	shard := &c.shards[int(hash%uint64(len(c.shards)))]

	tok := shard.mu.RLock()

	e, ok := shard.entries[hash]
	if !ok || (e.index >= shard.hand && e.gen != shard.gen-1) || (e.index < shard.hand && e.gen != shard.gen) {
		shard.mu.RUnlock(tok)
		return nil
	}

	header := (*header)(unsafe.Pointer(&shard.memory[e.index]))
	if header.hash != hash {
		// Corruption / Collision
		shard.mu.RUnlock(tok)
		return nil
	}

	if cap(buf) < header.size {
		buf = make([]byte, header.size)
	} else {
		buf = buf[:header.size]
	}

	copy(buf, shard.memory[e.index+headerSize:])
	shard.mu.RUnlock(tok)
	return buf
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

func (c *cache) Clear() {
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		shard.hand = 0
		shard.gen = 0
		shard.entries = make(map[uint64]entry)
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

func (c *cache) Serialize(w io.Writer) error {
	panic("unimplemented")
}

type entry struct {
	index int
	gen   int
}

type shard struct {
	mu      *rbMutex
	entries map[uint64]entry
	hand    int
	gen     int
	memory  []byte
}

func newShard(memory []byte) shard {
	return shard{
		mu:      newRBMutex(),
		entries: make(map[uint64]entry),
		memory:  memory,
	}
}

type header struct {
	hash uint64
	size int
}

var (
	headerSize = int(unsafe.Sizeof(header{}))
)
