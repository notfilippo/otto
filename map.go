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

// Modified version of https://github.com/puzpuzpuz/xsync/blob/main/map.go
// Licensed under Apache-2.0 Copyright 2025 Andrei Pechkurov

package otto

import (
	"sync"
)

const (
	// Default number of shards. Power of 2 for faster modulo.
	defaultShardCount = 256
)

// shard represents a single partition of the map.
type shard struct {
	sync.RWMutex
	m map[uint64]*entryHeader
}

// hmap is a sharded concurrent map from uint64 to *entryHeader.
// It uses multiple shards, each protected by a RWMutex, to reduce lock contention.
type hmap struct {
	shards     []shard
	shardCount uint64 // Must be power of 2
	shardMask  uint64 // shardCount - 1
}

// mapConfig defines configurable ShardMap options.
type mapConfig struct {
	sizeHint   int
	shardCount int
}

func newMap(sizeHint int) *hmap {
	c := &mapConfig{
		shardCount: defaultShardCount,
		sizeHint:   sizeHint,
	}

	m := &hmap{
		shards:     make([]shard, c.shardCount),
		shardCount: uint64(c.shardCount),
		shardMask:  uint64(c.shardCount - 1),
	}

	for i := range m.shards {
		m.shards[i].m = make(map[uint64]*entryHeader, c.sizeHint)
	}

	return m
}

// getShard returns the specific shard for a given key hash.
func (m *hmap) getShard(hash uint64) *shard {
	return &m.shards[hash&m.shardMask]
}

// Load returns the value stored in the map for a key hash, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *hmap) Load(hash uint64) (value *entryHeader, ok bool) {
	s := m.getShard(hash)
	s.RLock()
	value, ok = s.m[hash]
	s.RUnlock()
	return value, ok
}

// Store sets the value for a key hash.
func (m *hmap) Store(hash uint64, value *entryHeader) {
	s := m.getShard(hash)
	s.Lock()
	s.m[hash] = value
	s.Unlock()
}

// LoadOrStore returns the existing value for the key hash if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *hmap) LoadOrStore(hash uint64, value *entryHeader) (actual *entryHeader, loaded bool) {
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

// LoadAndStore returns the existing value for the key hash if present,
// while setting the new value for the key hash.
// It stores the new value and returns the existing one, if present.
// The loaded result is true if the existing value was loaded,
// false otherwise.
func (m *hmap) LoadAndStore(hash uint64, value *entryHeader) (actual *entryHeader, loaded bool) {
	s := m.getShard(hash)
	s.Lock()
	actual, loaded = s.m[hash]
	s.m[hash] = value
	s.Unlock()
	return actual, loaded
}

// LoadOrCompute returns the existing value for the key hash if present.
// Otherwise, it computes the value using the provided function, stores and
// returns the computed value. The loaded result is true if the value was loaded,
// false if computed.
func (m *hmap) LoadOrCompute(hash uint64, valueFn func() *entryHeader) (actual *entryHeader, loaded bool) {
	s := m.getShard(hash)
	// Optimistic read lock
	s.RLock()
	actual, loaded = s.m[hash]
	s.RUnlock()
	if loaded {
		return actual, true
	}

	// Need to compute, take write lock
	s.Lock()
	// Double check: Another goroutine might have inserted between RUnlock and Lock
	actual, loaded = s.m[hash]
	if !loaded {
		actual = valueFn()
		s.m[hash] = actual
	}
	s.Unlock()
	// If loaded is true here, it means another goroutine inserted the value.
	// If loaded is false, we computed and inserted it.
	return actual, loaded
}

// LoadOrTryCompute returns the existing value for the key hash if present.
// Otherwise, it tries to compute the value using the provided function
// and, if successful (cancel=false), stores and returns the computed value.
// The loaded result is true if the value was loaded. If the value was computed
// (successfully or not), loaded is false. If the compute attempt was cancelled
// (cancel=true), a nil value will be returned along with loaded=false.
func (m *hmap) LoadOrTryCompute(
	hash uint64,
	valueFn func() (newValue *entryHeader, cancel bool),
) (value *entryHeader, loaded bool) {
	s := m.getShard(hash)
	s.RLock()
	value, loaded = s.m[hash]
	s.RUnlock()
	if loaded {
		return value, true
	}

	s.Lock()
	value, loaded = s.m[hash] // Double check
	if !loaded {
		var cancel bool
		value, cancel = valueFn()
		if !cancel {
			s.m[hash] = value
		} else {
			value = nil // Ensure nil is returned if cancelled
		}
	}
	s.Unlock()
	// loaded is true if found during double-check, false otherwise.
	return value, loaded
}

// Compute updates the value for the key hash using the provided function.
// The function receives the old value (or nil) and whether it existed.
// It returns the new value and whether to delete the key.
// The ok result indicates whether a value (potentially nil) is present after the call.
func (m *hmap) Compute(
	hash uint64,
	valueFn func(oldValue *entryHeader, loaded bool) (newValue *entryHeader, delete bool),
) (actual *entryHeader, ok bool) {
	s := m.getShard(hash)
	s.Lock()
	oldValue, loaded := s.m[hash]
	newValue, del := valueFn(oldValue, loaded)
	if del {
		delete(s.m, hash)
		ok = false
		actual = nil // Explicitly set nil when deleted
	} else {
		s.m[hash] = newValue
		ok = true
		actual = newValue
	}
	s.Unlock()
	return actual, ok
}

// LoadAndDelete deletes the value for a key hash, returning the previous
// value if any. The loaded result reports whether the key hash was present.
func (m *hmap) LoadAndDelete(hash uint64) (value *entryHeader, loaded bool) {
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
func (m *hmap) Delete(hash uint64) {
	s := m.getShard(hash)
	s.Lock()
	delete(s.m, hash)
	s.Unlock()
}

// Range calls f sequentially for each key hash and value present in the
// map. If f returns false, range stops the iteration.
// Range acquires locks shard by shard. It does not represent a consistent
// snapshot of the map if modifications occur concurrently.
func (m *hmap) Range(f func(key uint64, value *entryHeader) bool) {
	for i := range m.shards {
		s := &m.shards[i]
		s.RLock()
		// Create a copy of the shard's map to iterate over without holding the lock
		// for the duration of the user-provided function f.
		// This prevents deadlocks if f tries to access the map again.
		shardCopy := make(map[uint64]*entryHeader, len(s.m))
		for k, v := range s.m {
			shardCopy[k] = v
		}
		s.RUnlock() // Release lock before calling f

		for k, v := range shardCopy {
			if !f(k, v) {
				return // Stop iteration if f returns false
			}
		}
	}
}

// Clear deletes all keys and values currently stored in the map.
func (m *hmap) Clear() {
	for i := range m.shards {
		s := &m.shards[i]
		s.Lock()
		s.m = make(map[uint64]*entryHeader) // Replace with new empty map
		s.Unlock()
	}
}

// Size returns current size (number of entries) of the map.
// It acquires locks shard by shard to calculate the total size.
func (m *hmap) Size() int {
	totalSize := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.RLock()
		totalSize += len(s.m)
		s.RUnlock()
	}
	return totalSize
}

// nextPowOf2 computes the next highest power of 2 of 32-bit v.
// Source: https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
// Kept for use in withShardCount option.
func nextPowOf2(v uint32) uint32 {
	if v == 0 {
		return 1 // Or handle as error/default case depending on requirement
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}
