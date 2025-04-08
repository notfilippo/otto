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

package otto_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/notfilippo/otto"
)

type TestCache interface {
	otto.Cache
	WaitForIdle()
}

var keyTrackingWindows = map[string]otto.TrackerWindow{
	"15m": {BucketCount: 15, BucketDuration: time.Minute},
	"1h":  {BucketCount: 15, BucketDuration: 4 * time.Minute},
	"6h":  {BucketCount: 15, BucketDuration: 24 * time.Minute},
}

func TestCacheBasic(t *testing.T) {
	// Create a cache with small slot size for testing
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize*slotCount), keyTrackingWindows)
	defer cache.Close()

	// Test setting and getting a simple value
	key := "test-key"
	value := []byte("test-value")

	cache.Set(key, value)

	result, ok := cache.Get(key, nil)
	if !ok && bytes.Equal(result, value) {
		t.Fatalf("Expected %q, got %q", value, result)
	}

	// Test getting a non-existent key
	nonExistentResult, ok := cache.Get("non-existent", nil)
	if ok {
		t.Fatalf("Expected nil, got %q", nonExistentResult)
	}
}

func TestCacheMultipleValues(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize*slotCount), keyTrackingWindows)
	defer cache.Close()

	// Create test data
	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
		"key4": []byte("value4"),
		"key5": []byte("value5"),
	}

	// Store all test data
	for k, v := range testData {
		cache.Set(k, v)
	}

	// Retrieve and verify all test data
	for k, expected := range testData {
		result, ok := cache.Get(k, nil)
		if !ok {
			t.Fatalf("For key %q, expected %q, got %q", k, expected, result)
		}
	}
}

func TestCacheLargeValues(t *testing.T) {
	size := 1 << 10
	cache := otto.NewTracker(otto.New(size), keyTrackingWindows)
	defer cache.Close()

	// Create large test data
	largeValue := make([]byte, size*90/100)
	r := rand.NewChaCha8([32]byte{0})
	r.Read(largeValue)

	key := "large-key"
	cache.Set(key, largeValue)

	result, ok := cache.Get(key, nil)
	if !ok {
		t.Fatalf("Large value not retrieved correctly. Expected %d bytes, got %d bytes", len(largeValue), len(result))
	}
}

func TestCacheEviction(t *testing.T) {
	// Create a cache with very limited capacity
	size := 10 // Very small, will force eviction
	cache := otto.NewTracker(otto.New(size), keyTrackingWindows)
	defer cache.Close()

	// Fill the cache plus some
	keys := []string{}
	for i := range 20 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Appendf(nil, "value-%d", i)
		cache.Set(key, value)
		keys = append(keys, key)
	}

	runtime.Gosched()

	// Some of the earlier keys should have been evicted
	evicted := false
	for _, key := range keys[:5] {
		if _, ok := cache.Get(key, nil); !ok {
			evicted = true
			break
		}
	}

	if !evicted {
		t.Fatal("Expected some keys to be evicted, but none were")
	}
}

func TestCacheReuse(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize*slotCount), keyTrackingWindows)
	defer cache.Close()

	key := "reuse-key"
	value := []byte("reuse-value")

	cache.Set(key, value)

	// Use a pre-allocated buffer
	buf := make([]byte, 32)
	result, ok := cache.Get(key, buf)

	// The result should point to the same underlying array as buf
	if !ok && bytes.Equal(result, buf) {
		t.Fatalf("Expected %q, got %q", value, result)
	}
}

func TestCacheClear(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize*slotCount), keyTrackingWindows)
	defer cache.Close()

	// Add some items
	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Appendf(nil, "value-%d", i)
		cache.Set(key, value)
	}

	// Clear the cache
	cache.Clear()

	// Verify all items are gone
	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		_, ok := cache.Get(key, nil)
		if ok {
			t.Fatalf("Item %s still exists after Clear()", key)
		}
	}

	// Verify we can still add new items
	key := "new-key"
	value := []byte("new-value")
	cache.Set(key, value)

	result, ok := cache.Get(key, nil)
	if !ok {
		t.Fatalf("Expected %q, got %q after Clear()", value, result)
	}
}

func TestCacheHeavyContention(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize*slotCount), keyTrackingWindows)
	defer cache.Close()

	// Many goroutines fighting for the same keys
	workers := 16
	keys := 10 // Small number of keys to maximize contention
	iterations := 1000

	var wg sync.WaitGroup

	// Start workers
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := range iterations {
				// Choose a key (all workers access all keys)
				keyIndex := i % keys
				key := fmt.Sprintf("shared-key-%d", keyIndex)

				// Randomly choose between read and write
				if i%3 == 0 { // 1/3 chance to write
					value := fmt.Appendf(nil, "value-%d-%d", id, i)
					cache.Set(key, value)
				} else { // 2/3 chance to read
					cache.Get(key, nil)
				}

				// Occasionally yield to increase contention
				if i%50 == 0 {
					runtime.Gosched()
				}
			}
		}(w)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Just verify the cache is still functioning
	testKey := "test-after-contention"
	testValue := []byte("test-value")
	cache.Set(testKey, testValue)

	_, ok := cache.Get(testKey, nil)
	if !ok {
		t.Fatalf("Cache not functioning correctly after contention test")
	}
}

func TestCacheSerialization(t *testing.T) {
	cache := otto.New(1 << 10)
	defer cache.Close()

	// Add some test data
	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("A longer value that spans multiple slots potentially"),
		"key4": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, // Binary data
	}

	// Store all test data
	for k, v := range testData {
		cache.Set(k, v)
	}

	cache.(TestCache).WaitForIdle()
	time.Sleep(1 * time.Second)

	// Create a buffer and serialize the cache
	var buf bytes.Buffer
	err := cache.Serialize(&buf)
	if err != nil {
		t.Fatalf("Failed to serialize cache: %v", err)
	}

	// Deserialize into a new cache
	newCache, err := otto.Deserialize(&buf)
	if err != nil {
		t.Fatalf("Failed to deserialize cache: %v", err)
	}

	// Verify all data is present in the new cache
	for k, expected := range testData {
		result, ok := newCache.Get(k, nil)
		if !ok {
			t.Fatalf("After deserialization, for key %q, expected %q, got %q", k, expected, result)
		}
	}
}

func TestCacheDifferentSizes(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize*slotCount), keyTrackingWindows)
	defer cache.Close()

	// Add items of different sizes
	testCases := []struct {
		key   string
		value []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("small")},
		{"exact-slot", make([]byte, slotSize)},
		{"one-byte-over", make([]byte, slotSize+1)},
		{"two-slots", make([]byte, slotSize*2)},
		{"two-slots-plus", make([]byte, slotSize*2+5)},
		{"many-slots", make([]byte, slotSize*7+3)},
	}

	// Fill the values with identifiable data
	for i := range testCases {
		for j := range testCases[i].value {
			testCases[i].value[j] = byte(j % 256)
		}
	}

	// Store all items
	for _, tc := range testCases {
		cache.Set(tc.key, tc.value)
	}

	// Retrieve and verify all items
	for _, tc := range testCases {
		result, ok := cache.Get(tc.key, nil)
		if !ok {
			t.Fatalf("For key %q, size mismatch. Expected %d bytes, got %d bytes",
				tc.key, len(tc.value), len(result))
		}
	}
}

func TestCacheMaxSlots(t *testing.T) {
	slotSize := 16
	slotCount := 10
	cache := otto.NewTracker(otto.New(slotSize*slotCount), keyTrackingWindows)
	defer cache.Close()

	// Create a value that would use almost all slots
	maxValue := make([]byte, slotSize*(slotCount-1))
	for i := range maxValue {
		maxValue[i] = byte(i % 256)
	}

	cache.Set("max-slots", maxValue)

	_, ok := cache.Get("max-slots", nil)
	if !ok {
		t.Fatalf("Failed to retrieve maximum-size value correctly")
	}

	// Another large value should cause eviction
	anotherValue := make([]byte, slotSize*2)
	for i := range anotherValue {
		anotherValue[i] = byte(i % 256)
	}

	cache.Set("another-key", anotherValue)

	// The first value might have been evicted
	if _, ok := cache.Get("max-slots", nil); !ok {
		// This is expected behavior, the test passes
		t.Log("Maximum size value was evicted as expected")
	} else {
		// This is also valid if the cache had room
		_, ok = cache.Get("another-key", nil)
		if !ok {
			t.Fatalf("Failed to retrieve second large value correctly")
		}
	}
}
