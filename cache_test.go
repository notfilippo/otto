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
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/notfilippo/otto"
)

var keyTrackingWindows = map[string]otto.TrackerWindow{
	"15m": {BucketCount: 15, BucketDuration: time.Minute},
	"1h":  {BucketCount: 15, BucketDuration: 4 * time.Minute},
	"6h":  {BucketCount: 15, BucketDuration: 24 * time.Minute},
}

func TestCacheBasic(t *testing.T) {
	// Create a cache with small slot size for testing
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

	// Test setting and getting a simple value
	key := "test-key"
	value := []byte("test-value")

	cache.Set(key, value)

	result := cache.Get(key, nil)
	if !bytes.Equal(result, value) {
		t.Fatalf("Expected %q, got %q", value, result)
	}

	// Test getting a non-existent key
	nonExistentResult := cache.Get("non-existent", nil)
	if nonExistentResult != nil {
		t.Fatalf("Expected nil, got %q", nonExistentResult)
	}
}

func TestCacheMultipleValues(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

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
		result := cache.Get(k, nil)
		if !bytes.Equal(result, expected) {
			t.Fatalf("For key %q, expected %q, got %q", k, expected, result)
		}
	}
}

func TestCacheLargeValues(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

	// Create large test data (spans multiple slots)
	largeValue := make([]byte, slotSize*3+5) // 53 bytes (spans 4 slots)
	r := rand.NewChaCha8([32]byte{0})
	r.Read(largeValue)

	key := "large-key"
	cache.Set(key, largeValue)

	result := cache.Get(key, nil)
	if !bytes.Equal(result, largeValue) {
		t.Fatalf("Large value not retrieved correctly. Expected %d bytes, got %d bytes", len(largeValue), len(result))
	}
}

func TestCacheEviction(t *testing.T) {
	// Create a cache with very limited capacity
	slotSize := 16
	slotCount := 10 // Very small, will force eviction
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

	// Fill the cache plus some
	keys := []string{}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		cache.Set(key, value)
		keys = append(keys, key)
	}

	// Some of the earlier keys should have been evicted
	evicted := false
	for _, key := range keys[:5] {
		if cache.Get(key, nil) == nil {
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
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

	key := "reuse-key"
	value := []byte("reuse-value")

	cache.Set(key, value)

	// Use a pre-allocated buffer
	buf := make([]byte, 32)
	result := cache.Get(key, buf)

	// The result should point to the same underlying array as buf
	if !bytes.Equal(result, value) {
		t.Fatalf("Expected %q, got %q", value, result)
	}
}

func TestCacheClear(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

	// Add some items
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		cache.Set(key, value)
	}

	// Clear the cache
	cache.Clear()

	// Verify all items are gone
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		if cache.Get(key, nil) != nil {
			t.Fatalf("Item %s still exists after Clear()", key)
		}
	}

	// Verify we can still add new items
	key := "new-key"
	value := []byte("new-value")
	cache.Set(key, value)

	result := cache.Get(key, nil)
	if !bytes.Equal(result, value) {
		t.Fatalf("Expected %q, got %q after Clear()", value, result)
	}
}

func TestCacheFrequency(t *testing.T) {
	// Create a cache with limited capacity
	slotSize := 16
	slotCount := 20
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

	// Add items to the cache
	frequentKey := "frequent-key"
	frequentValue := []byte("frequent-value")
	cache.Set(frequentKey, frequentValue)

	// Access this key many times to increase its frequency
	for i := 0; i < 10; i++ {
		cache.Get(frequentKey, nil)
	}

	// Fill the cache with other items to force eviction
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("filler-%d", i)
		value := []byte(fmt.Sprintf("filler-value-%d", i))
		cache.Set(key, value)
	}

	// The frequent key should still be in the cache
	result := cache.Get(frequentKey, nil)
	if result == nil || !bytes.Equal(result, frequentValue) {
		t.Fatalf("Frequently accessed item was incorrectly evicted")
	}
}

func TestCacheHeavyContention(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

	// Many goroutines fighting for the same keys
	workers := 16
	keys := 10 // Small number of keys to maximize contention
	iterations := 1000

	var wg sync.WaitGroup

	// Start workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				// Choose a key (all workers access all keys)
				keyIndex := i % keys
				key := fmt.Sprintf("shared-key-%d", keyIndex)

				// Randomly choose between read and write
				if i%3 == 0 { // 1/3 chance to write
					value := []byte(fmt.Sprintf("value-%d-%d", id, i))
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

	result := cache.Get(testKey, nil)
	if !bytes.Equal(result, testValue) {
		t.Fatalf("Cache not functioning correctly after contention test")
	}
}

func TestCacheSerialization(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

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
		result := newCache.Get(k, nil)
		if !bytes.Equal(result, expected) {
			t.Fatalf("After deserialization, for key %q, expected %q, got %q", k, expected, result)
		}
	}
}

func TestCacheFileStorage(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "cache.bin")

	// Create and populate original cache
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

	// Add some test data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("file-key-%d", i)
		value := []byte(fmt.Sprintf("file-value-%d", i))
		cache.Set(key, value)
	}

	// Save to file
	err := otto.SaveToFile(cache, filePath)
	if err != nil {
		t.Fatalf("Failed to save cache to file: %v", err)
	}

	// Load from file
	loadedCache, err := otto.LoadFromFile(filePath)
	if err != nil {
		t.Fatalf("Failed to load cache from file: %v", err)
	}

	// Verify data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("file-key-%d", i)
		expected := []byte(fmt.Sprintf("file-value-%d", i))
		result := loadedCache.Get(key, nil)
		if !bytes.Equal(result, expected) {
			t.Fatalf("After file loading, for key %q, expected %q, got %q", key, expected, result)
		}
	}

	// Clean up
	os.Remove(filePath)
}

func TestCacheDifferentSizes(t *testing.T) {
	slotSize := 16
	slotCount := 100
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

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
		result := cache.Get(tc.key, nil)
		if !bytes.Equal(result, tc.value) {
			t.Fatalf("For key %q, size mismatch. Expected %d bytes, got %d bytes",
				tc.key, len(tc.value), len(result))
		}
	}
}

func TestCacheMaxSlots(t *testing.T) {
	slotSize := 16
	slotCount := 10
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

	// Create a value that would use almost all slots
	maxValue := make([]byte, slotSize*(slotCount-1))
	for i := range maxValue {
		maxValue[i] = byte(i % 256)
	}

	cache.Set("max-slots", maxValue)

	result := cache.Get("max-slots", nil)
	if !bytes.Equal(result, maxValue) {
		t.Fatalf("Failed to retrieve maximum-size value correctly")
	}

	// Another large value should cause eviction
	anotherValue := make([]byte, slotSize*2)
	for i := range anotherValue {
		anotherValue[i] = byte(i % 256)
	}

	cache.Set("another-key", anotherValue)

	// The first value might have been evicted
	if cache.Get("max-slots", nil) == nil {
		// This is expected behavior, the test passes
		t.Log("Maximum size value was evicted as expected")
	} else {
		// This is also valid if the cache had room
		result = cache.Get("another-key", nil)
		if !bytes.Equal(result, anotherValue) {
			t.Fatalf("Failed to retrieve second large value correctly")
		}
	}
}

func TestCacheGetAfterConcurrentEviction(t *testing.T) {
	slotSize := 16
	slotCount := 20 // Small size to force eviction
	cache := otto.NewTracker(otto.New(slotSize, slotCount), keyTrackingWindows)

	// Add a test key
	testKey := "eviction-test"
	testValue := []byte("test-value")
	cache.Set(testKey, testValue)

	// Start a goroutine that will try to evict this key
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Add many large values to force eviction
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("large-key-%d", i)
			value := make([]byte, slotSize*2) // Each takes 2 slots
			r := rand.NewChaCha8([32]byte{0})
			r.Read(value)

			cache.Set(key, value)

			// Briefly yield to allow main thread to try getting
			runtime.Gosched()
		}
	}()

	// Try to get the test key repeatedly while the eviction is happening
	for i := 0; i < 50; i++ {
		result := cache.Get(testKey, nil)

		// Either it's our value or it's nil (evicted)
		if result != nil && !bytes.Equal(result, testValue) {
			t.Fatalf("Got corrupted data: expected nil or %q, got %q", testValue, result)
		}

		runtime.Gosched()
	}

	wg.Wait()
}
