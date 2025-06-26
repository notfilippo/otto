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
	"maps"
	"sync"
)

// shard represents a single partition of the map.
type shard[T any] struct {
	sync.RWMutex
	m map[uint64]T
}

// hmap is a sharded concurrent map from uint64 to T.
// It uses multiple shards, each protected by a RWMutex, to reduce lock contention.
type hmap[T any] struct {
	shards []shard[T]
}

var (
	shardCount = int(nextPowOf2(32 * parallelism()))
	shardMask  = uint64(shardCount - 1)
)

func newMap[T any](sizeHint int) *hmap[T] {
	m := &hmap[T]{
		shards: make([]shard[T], shardCount),
	}

	for i := range m.shards {
		m.shards[i].m = make(map[uint64]T, sizeHint/shardCount)
	}

	return m
}

// getShard returns the specific shard for a given key hash.
func (m *hmap[T]) getShard(hash uint64) *shard[T] {
	return &m.shards[hash&shardMask]
}

// Load returns the value stored in the map for a key hash, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *hmap[T]) Load(hash uint64) (value T, ok bool) {
	s := m.getShard(hash)
	s.RLock()
	value, ok = s.m[hash]
	s.RUnlock()
	return value, ok
}

// Store sets the value for a key hash.
func (m *hmap[T]) Store(hash uint64, value T) {
	s := m.getShard(hash)
	s.Lock()
	s.m[hash] = value
	s.Unlock()
}

// LoadOrStore returns the existing value for the key hash if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *hmap[T]) LoadOrStore(hash uint64, value T) (actual T, loaded bool) {
	s := m.getShard(hash)
	s.Lock()
	actual, loaded = s.m[hash]
	if !loaded {
		actual = value
		s.m[hash] = value
	}
	s.Unlock()
	return actual, loaded
}

// LoadAndDelete deletes the value for a key hash, returning the previous
// value if any. The loaded result reports whether the key hash was present.
func (m *hmap[T]) LoadAndDelete(hash uint64) (value T, loaded bool) {
	s := m.getShard(hash)
	s.Lock()
	value, loaded = s.m[hash]
	if loaded {
		delete(s.m, hash)
	}
	s.Unlock()
	return value, loaded
}

// Delete deletes the value for a key hash.
func (m *hmap[T]) Delete(hash uint64) {
	s := m.getShard(hash)
	s.Lock()
	delete(s.m, hash)
	s.Unlock()
}

// Range calls f sequentially for each key hash and value present in the
// map. If f returns error, range stops the iteration and returns that error.
// Range acquires locks shard by shard. It does not represent a consistent
// snapshot of the map if modifications occur concurrently.
func (m *hmap[T]) Range(f func(key uint64, value T) error) error {
	for i := range m.shards {
		s := &m.shards[i]
		s.RLock()
		// Create a copy of the shard's map to iterate over without holding the lock
		// for the duration of the user-provided function f.
		// This prevents deadlocks if f tries to access the map again.
		shardCopy := make(map[uint64]T, len(s.m))
		maps.Copy(shardCopy, s.m)
		s.RUnlock() // Release lock before calling f

		for k, v := range shardCopy {
			if err := f(k, v); err != nil {
				return err // Stop iteration if f returns error
			}
		}
	}

	return nil
}

// Clear deletes all keys and values currently stored in the map.
func (m *hmap[T]) Clear() {
	for i := range m.shards {
		s := &m.shards[i]
		s.Lock()
		s.m = make(map[uint64]T) // Replace with new empty map
		s.Unlock()
	}
}

// Size returns current size (number of entries) of the map.
// It acquires locks shard by shard to calculate the total size.
func (m *hmap[T]) Size() int {
	totalSize := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.RLock()
		totalSize += len(s.m)
		s.RUnlock()
	}
	return totalSize
}
