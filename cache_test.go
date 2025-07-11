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
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"unsafe"
)

func newCache(tb testing.TB, slotSize, mCapacity, sCapacity int32) Cache {
	cache := NewEx(slotSize, mCapacity, sCapacity)
	tb.Cleanup(func() {
		if err := cache.Close(); err != nil {
			tb.Fatalf("failed to close cache: %v", err)
		}
	})
	return cache
}

const (
	testSlotSize  = 16
	testMCapacity = 9000
	testSCapacity = 1000
)

func defaultCache(tb testing.TB) Cache {
	return newCache(tb, testSlotSize, testMCapacity, testSCapacity)
}

func key(i int) string {
	return fmt.Sprintf("key-%d", i)
}

func rng(i int) *rand.ChaCha8 {
	init := [32]byte{}
	binary.NativeEndian.PutUint64(init[:], uint64(i))
	return rand.NewChaCha8(init)
}

func value(i int, slots int) []byte {
	size := slots * (testSlotSize - 8)
	buf := make([]byte, size)
	rng(i).Read(buf)
	return buf
}

// cacheSet is a shortcut to set a key in the cache.
func cacheSet(_ testing.TB, cache Cache, i int, slots int) {
	cache.Set(key(i), value(i, slots))
}

// cacheHit is a shortcut to check if a key is in the cache.
func cacheHit(tb testing.TB, cache Cache, i int, slots int) {
	v := cache.Get(key(i), nil)
	if v == nil {
		tb.Fatalf("expected key-%d to be in cache", i)
	}
	expected := value(i, slots)
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
	cacheSet(t, c, 1, 2)
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
	cacheHit(t, c, 1, 2)
}

func TestSerializeEmpty(t *testing.T) {
	c := defaultCache(t)
	var buf bytes.Buffer
	err := c.Serialize(&buf)
	if err != nil {
		t.Fatalf("failed to serialize cache: %v", err)
	}

	c, err = DeserializeEx(&buf, testSlotSize, testMCapacity, testSCapacity)
	if err != nil {
		t.Fatalf("failed to deserialize cache: %v", err)
	}
	cacheMiss(t, c, 0)
}

func TestV100Serialize(t *testing.T) {
	c := defaultCache(t)
	cacheSet(t, c, 0, 1)
	cacheSet(t, c, 1, 2)
	var buf bytes.Buffer
	err := c.(*cache).SerializeV100(&buf)
	if err != nil {
		t.Fatalf("failed to serialize cache v100: %v", err)
	}

	c, err = DeserializeEx(&buf, testSlotSize, testMCapacity, testSCapacity)
	if err != nil {
		t.Fatalf("failed to deserialize cache V100: %v", err)
	}
	cacheHit(t, c, 0, 1)
	cacheHit(t, c, 1, 2)
}

func TestSerializeComplexCache(t *testing.T) {
	const (
		testSlotSize  = 16
		testMCapacity = 100
		testSCapacity = 50
		prepareRounds = 60000
		maxKeys       = 5000
		maxSlots      = 8
	)

	c := newCache(t, testSlotSize, testMCapacity, testSCapacity)
	r := rng(0)
	rawSeed := r.Uint64()
	seed := *(*maphash.Seed)(unsafe.Pointer(&rawSeed))

	// Add some random entries to the ghost cache
	for i := 0; i < prepareRounds/10; i++ {
		keyIdx := int(r.Uint64() % maxKeys)
		cacheSet(t, c, keyIdx, keySlots(seed, keyIdx, maxSlots))
	}

	// Run some random insertion / read rounds to have a complex cache state
	for i := 0; i < prepareRounds; i++ {
		keyIdx := int(r.Uint64() % maxKeys)
		// 1/2 of operations are read
		if r.Uint64()%2 == 0 {
			cacheSet(t, c, keyIdx, keySlots(seed, keyIdx, maxSlots))
		} else {
			c.Get(key(keyIdx), nil)
		}
	}

	// Record all entries that should be present in the cache
	// We'll check these after serialization/deserialization
	expectedEntries := make(map[string]int) // key -> expected slots

	// Check which entries are actually in the cache and record them
	for i := 0; i < maxKeys; i++ {
		key := key(i)
		if c.Has(key) {
			expectedEntries[key] = keySlots(seed, i, maxSlots)
		}
	}

	// Verify we have a reasonable number of entries
	if len(expectedEntries) == 0 {
		t.Fatal("no entries found in cache before serialization")
	}

	t.Logf("Cache has %d entries before serialization", len(expectedEntries))

	// Serialize the cache
	var buf bytes.Buffer
	err := c.Serialize(&buf)
	if err != nil {
		t.Fatalf("failed to serialize cache: %v", err)
	}

	// Deserialize the cache
	deserializedCache, err := DeserializeEx(&buf, testSlotSize, testMCapacity, testSCapacity)
	if err != nil {
		t.Fatalf("failed to deserialize cache: %v", err)
	}

	// Verify all expected entries are still present with correct values
	for key, expectedSlots := range expectedEntries {
		expectedValue := value(extractKeyNumber(key), expectedSlots)
		actualValue := deserializedCache.Get(key, nil)

		if actualValue == nil {
			t.Errorf("entry %s missing after deserialization", key)
			continue
		}

		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("entry %s has wrong value after deserialization: expected %x, got %x",
				key, expectedValue, actualValue)
		}
	}

	// Also verify that entries that weren't in the cache before are still not there
	for i := 0; i < maxKeys; i++ {
		key := key(i)
		if _, exists := expectedEntries[key]; !exists && deserializedCache.Has(key) {
			t.Errorf("unexpected entry %s found after deserialization", key)
		}
	}

	// Verify ghost cache was also serialized
	originalGhostBits := c.(*cache).g.filter.bits
	deserializedGhostBits := deserializedCache.(*cache).g.filter.bits
	if !bytes.Equal(originalGhostBits, deserializedGhostBits) {
		t.Errorf("ghost bits have wrong value after deserialization: expected %x, got %x", originalGhostBits, deserializedGhostBits)
	}

	t.Logf("Successfully verified %d entries preserved after serialization/deserialization", len(expectedEntries))
}

func keySlots(seed maphash.Seed, key int, maxSlots int) int {
	var n int64 = int64(key)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf, uint32(n))
	return int(maphash.Bytes(seed, buf) % uint64(maxSlots))
}

// extractKeyNumber extracts the number from a key like "key-123"
func extractKeyNumber(key string) int {
	var num int
	fmt.Sscanf(key, "key-%d", &num)
	return num
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
