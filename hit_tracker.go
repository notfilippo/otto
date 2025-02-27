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

// Modified version of https://github.com/puzpuzpuz/xsync/blob/main/mpmcqueue.go
// Licensed under Apache-2.0 Copyright 2025 Andrei Pechkurov

package otto

import (
	"hash/maphash"
	"io"
	"sync/atomic"
)

var _ Cache = (*TrackerCache)(nil)

type TrackerCache struct {
	inner Cache

	seed maphash.Seed
	get, set chan uint64

	hits, misses, sets atomic.Uint64
}

func NewTracker(c Cache) *TrackerCache {
	tracker := &TrackerCache{
		inner: c,
		seed: maphash.MakeSeed(),
		get: make(chan uint64, 128),
		set: make(chan uint64, 128),
	}
	go tracker.loop()
	return tracker
}

func (s *TrackerCache) loop() {
	
}

// Returns 0 if n >= 1, otherwise 1
func isZero(n uint64) uint64 {
	return (n | (n - 1)) >> 63
}

func (s *TrackerCache) Get(key string, buf []byte) []byte {
	result := s.inner.Get(key, buf)
	size := uint64(len(result))
	s.hits.Add(1-isZero(size))
	s.misses.Add(isZero(size))
	return result
}

func (s *TrackerCache) Set(key string, val []byte) {
	s.sets.Add(1)
	s.inner.Set(key, val)
}

func (s *TrackerCache) Clear() {
	s.hits.Store(0)
	s.misses.Store(0)
	s.sets.Store(0)

	s.inner.Clear()
}

func (s *TrackerCache) Close() {
	s.inner.Close()
}

func (s *TrackerCache) Entries() uint64 {
	return s.inner.Entries()
}

func (s *TrackerCache) Serialize(w io.Writer) error {
	return s.Serialize(w)
}

func (s *TrackerCache) Hits() uint64 {
	return s.hits.Swap(0)
}

func (s *TrackerCache) Misses() uint64 {
	return s.misses.Swap(0)
}

func (s *TrackerCache) Sets() uint64 {
	return s.sets.Swap(0)
}
