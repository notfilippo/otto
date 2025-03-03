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
	"hash/maphash"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var _ Cache = (*TrackerCache)(nil)

type TrackerCache struct {
	inner Cache

	seed maphash.Seed
	seen chan uint64

	hits, misses, sets, dropped atomic.Uint64

	windows sync.Map
}

func NewTracker(c Cache, windows map[string]TrackerWindow) *TrackerCache {
	tracker := &TrackerCache{
		inner: c,
		seed:  maphash.MakeSeed(),
		seen:  make(chan uint64, 128),
	}

	if len(windows) > 0 {
		go tracker.loop(windows)
	}

	return tracker
}

func (s *TrackerCache) loop(windows map[string]TrackerWindow) {
	minimumDuration := time.Duration(math.MaxInt64)
	for _, window := range windows {
		minimumDuration = min(window.BucketDuration, minimumDuration)
	}

	tracker := newSlidingTracker(windows)

	ticker := time.NewTicker(minimumDuration / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for window, count := range tracker.stats() {
				s.windows.Store(window, count)
			}
		case k := <-s.seen:
			if k == 0 {
				return
			}

			tracker.track(k)
		}
	}
}

func (s *TrackerCache) Get(key string, buf []byte) []byte {
	result := s.inner.Get(key, buf)

	if len(result) > 0 {
		s.hits.Add(1)

		select {
		case s.seen <- maphash.String(s.seed, key):
		default:
			s.dropped.Add(1)
		}
	} else {
		s.misses.Add(1)
	}

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
	return s.inner.Serialize(w)
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

func (s *TrackerCache) Window(name string) (uint64, bool) {
	value, ok := s.windows.Load(name)
	if !ok {
		return 0, false
	}

	return value.(uint64), true
}
