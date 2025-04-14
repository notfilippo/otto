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
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
)

func newCache(tb testing.TB, slotSize, mCapacity, sCapacity int) Cache {
	cache := NewEx(slotSize, mCapacity, sCapacity)
	tb.Cleanup(func() {
		if err := cache.Close(); err != nil {
			tb.Fatalf("failed to close cache: %v", err)
		}
	})
	return cache
}

var (
	testSlotSize  = 16
	testMCapacity = 90
	testSCapacity = 10
)

func defaultCache(tb testing.TB) Cache {
	return newCache(tb, testSlotSize, testMCapacity, testSCapacity)
}

func key(i int) string {
	return fmt.Sprintf("key-%d", i)
}

func value(tb testing.TB, i int, slots int) []byte {
	size := slots * testSlotSize
	single := fmt.Appendf(nil, "value-%d", i)
	if len(single) > size {
		tb.Fatalf("value-%d is too long, max is %d", i, size)
	}
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = single[i%len(single)]
	}
	return buf
}

// cacheSet is a shortcut to set a key in the cache.
func cacheSet(tb testing.TB, cache Cache, i int, slots int) {
	cache.Set(key(i), value(tb, i, slots))
}

// cacheHit is a shortcut to check if a key is in the cache.
func cacheHit(tb testing.TB, cache Cache, i int, slots int) {
	v := cache.Get(key(i), nil)
	if v == nil {
		tb.Fatalf("expected key-%d to be in cache", i)
	}
	expected := value(tb, i, slots)
	if !bytes.Equal(v, expected) {
		tb.Fatalf("expected key-%d to have value %x instead found value %x", i, expected, v)
	}
}

// cacheMiss is a shortcut to check if a key is not in the cache.
func cacheMiss(tb testing.TB, cache Cache, i int) {
	v := cache.Get(key(i), nil)
	if v != nil {
		tb.Fatalf("expected key-%d to NOT be in cache (found: %s)", i, v)
	}
}

func TestSanity(t *testing.T) {
	c := defaultCache(t)

	for i := range testSCapacity + testMCapacity - 1 {
		cacheSet(t, c, i, i+1)
		cacheHit(t, c, i, i+1)
	}
}

func TestEvictionFillCapacity(t *testing.T) {
	c := defaultCache(t)
	for i := range testSCapacity {
		cacheSet(t, c, i, 1)
	}

	for i := range testSCapacity {
		cacheHit(t, c, i, 1)
	}
}

func TestEvictionSQueue(t *testing.T) {
	c := defaultCache(t)
	for i := range testSCapacity {
		cacheSet(t, c, i, 1)
	}

	cacheSet(t, c, testSCapacity, 1)
	cacheMiss(t, c, 0)
}

func TestEvictionPromotion(t *testing.T) {
	c := defaultCache(t)
	for i := range testSCapacity {
		cacheSet(t, c, i, 1)
	}

	cacheHit(t, c, 0, 1)
	cacheHit(t, c, 0, 1)

	cacheSet(t, c, testSCapacity, 1)

	// key-0 should've been promoted to the m-queue.
	cacheHit(t, c, 0, 1)
	// ... and key-1 should've been evicted to make
	// space for the Set.
	cacheMiss(t, c, 1)
}

func TestEvictionMultiSlot(t *testing.T) {
	c := defaultCache(t)
	for i := range testSCapacity {
		cacheSet(t, c, i, 1)
	}

	cacheSet(t, c, testSCapacity, 2)

	// key-0 should've been evicted from s-queue.
	cacheMiss(t, c, 0)
	// ... but! key-1 still remains in s-queue even though
	// the size of the queue now is testSCapacity + 1.
	// This is expected and the queue will report its full
	// capacity in the next eviction cycle.
	cacheHit(t, c, 1, 1)

	// Finally, we should've stored correctly the new entry.
	cacheHit(t, c, testSCapacity, 2)
}

func TestEvictionMQueue(t *testing.T) {
	c := defaultCache(t)

	for i := range testMCapacity {
		cacheSet(t, c, i, 1)
		cacheHit(t, c, i, 1)
		cacheHit(t, c, i, 1)
	}

	for i := range testSCapacity {
		cacheSet(t, c, i+testMCapacity, 1)
	}

	// The cache is now full with frequency for all entries
	// set to 3.

	for i := range testSCapacity + testMCapacity {
		cacheHit(t, c, i, 1)
	}

	cacheSet(t, c, testSCapacity+testMCapacity, 1)

	// The first key of the s-queue should be evicted.
	cacheMiss(t, c, testMCapacity)

	// ... while the first key of the m-queue should still
	// be there.
	cacheHit(t, c, 0, 1)
}

func TestClear(t *testing.T) {
	c := defaultCache(t)
	cacheSet(t, c, 0, 1)
	cacheHit(t, c, 0, 1)
	c.Clear()
	cacheMiss(t, c, 0)
}

func TestSerialize(t *testing.T) {
	c := defaultCache(t)
	cacheSet(t, c, 0, 1)
	var buf bytes.Buffer
	err := c.Serialize(&buf)
	if err != nil {
		t.Fatalf("failed to serialize cache: %v", err)
	}

	c, err = DeserializeEx(&buf, testSlotSize, testMCapacity, testSCapacity)
	if err != nil {
		t.Fatalf("failed to deserialize cache: %v", err)
	}
	cacheHit(t, c, 0, 1)
}

func BenchmarkConcurrentGet(b *testing.B) {
	c := defaultCache(b)

	cacheSet(b, c, 0, 1)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cacheHit(b, c, 0, 1)
		}
	})
}

func BenchmarkConcurrentSet(b *testing.B) {
	c := defaultCache(b)
	var counter int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1)
			cacheSet(b, c, int(i), 1)
		}
	})
}
