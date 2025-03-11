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
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueTryDequeueBatch_Basic(t *testing.T) {
	q := newQueue[int](10)

	// Fill the queue with some items
	for i := range 5 {
		if !q.TryEnqueue(i) {
			t.Fatalf("Failed to enqueue item %d", i)
		}
	}

	var items []int
	// Dequeue a batch of 3 items
	ok := q.TryDequeueBatch(3, func(item int) { items = append(items, item) })
	if !ok {
		t.Fatal("Failed to dequeue batch")
	}

	if len(items) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(items))
	}

	// Check the values
	for i, item := range items {
		if item != i {
			t.Fatalf("Expected item %d, got %d", i, item)
		}
	}

	// Should have 2 items left
	item, ok := q.TryDequeue()
	if !ok {
		t.Fatal("Failed to dequeue item")
	}
	if item != 3 {
		t.Fatalf("Expected item 3, got %d", item)
	}

	item, ok = q.TryDequeue()
	if !ok {
		t.Fatal("Failed to dequeue item")
	}
	if item != 4 {
		t.Fatalf("Expected item 4, got %d", item)
	}

	// Queue should be empty now
	_, ok = q.TryDequeue()
	if ok {
		t.Fatal("Expected queue to be empty")
	}
}

func TestQueueTryDequeueBatch_NotEnoughItems(t *testing.T) {
	q := newQueue[int](10)

	// Fill the queue with some items
	for i := range 3 {
		if !q.TryEnqueue(i) {
			t.Fatalf("Failed to enqueue item %d", i)
		}
	}

	// Try to dequeue a batch of 5 items (should fail)
	ok := q.TryDequeueBatch(5, func(item int) {})
	if ok {
		t.Fatal("Expected batch dequeue to fail")
	}

	// Original items should still be in the queue
	for i := range 3 {
		item, ok := q.TryDequeue()
		if !ok {
			t.Fatal("Failed to dequeue item")
		}
		if item != i {
			t.Fatalf("Expected item %d, got %d", i, item)
		}
	}
}

func TestQueueTryDequeueBatch_EmptyQueue(t *testing.T) {
	q := newQueue[int](10)

	ok := q.TryDequeueBatch(1, func(item int) {})
	if ok {
		t.Fatal("Expected batch dequeue to fail on empty queue")
	}
}

func TestQueueTryDequeueBatch_InvalidSize(t *testing.T) {
	q := newQueue[int](10)

	// Fill the queue
	for i := range 5 {
		q.TryEnqueue(i)
	}

	// Try to dequeue a batch of 0 items (should fail)
	ok := q.TryDequeueBatch(0, func(item int) {})
	if ok {
		t.Fatal("Expected batch dequeue to fail with size 0")
	}

	// Try to dequeue a batch of -1 items (should fail)
	ok = q.TryDequeueBatch(-1, func(item int) {})
	if ok {
		t.Fatal("Expected batch dequeue to fail with negative size")
	}

	// Original items should still be in the queue
	for i := range 5 {
		item, ok := q.TryDequeue()
		if !ok {
			t.Fatal("Failed to dequeue item")
		}
		if item != i {
			t.Fatalf("Expected item %d, got %d", i, item)
		}
	}
}

func TestQueueTryDequeueBatch_Concurrent(t *testing.T) {
	const (
		queueSize     = 100
		itemCount     = 10000
		batchSize     = 10
		consumerCount = 5
	)

	q := newQueue[int](queueSize)

	// Keep track of dequeued items
	var dequeuedCount int64
	var wg sync.WaitGroup

	// Start consumers that will try to dequeue batches
	for range consumerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var items []int
				ok := q.TryDequeueBatch(batchSize, func(item int) { items = append(items, item) })
				if ok {
					atomic.AddInt64(&dequeuedCount, int64(len(items)))
				} else {
					// Check if we're done
					if atomic.LoadInt64(&dequeuedCount) >= itemCount {
						break
					}
					// Give other goroutines a chance
					runtime.Gosched()
				}
			}
		}()
	}

	// Produce items
	for i := range itemCount {
		for !q.TryEnqueue(i) {
			// Queue is full, yield to let consumers catch up
			runtime.Gosched()
		}
	}

	// Wait for consumers to finish
	wg.Wait()

	// Verify all items were dequeued
	if dequeuedCount != itemCount {
		t.Fatalf("Expected %d dequeued items, got %d", itemCount, dequeuedCount)
	}
}

func TestQueueTryDequeueBatch_MixedOperations(t *testing.T) {
	const (
		queueSize     = 100
		itemCount     = 10000
		producerCount = 5
		consumerCount = 5
		batcherCount  = 3
		maxBatchSize  = 8
	)

	q := newQueue[string](queueSize)

	var (
		enqueueCount int64
		dequeueCount int64
		batchCount   int64
		wg           sync.WaitGroup
	)

	// Start producers
	for i := range producerCount {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; atomic.LoadInt64(&enqueueCount) < itemCount; j++ {
				if q.TryEnqueue(fmt.Sprintf("p%d-i%d", id, j)) {
					atomic.AddInt64(&enqueueCount, 1)
				} else {
					runtime.Gosched()
				}
			}
		}(i)
	}

	// Start single-item consumers
	for range consumerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, ok := q.TryDequeue()
				if ok {
					atomic.AddInt64(&dequeueCount, 1)
				} else {
					// Check if we're done
					total := atomic.LoadInt64(&dequeueCount) + atomic.LoadInt64(&batchCount)
					if total >= itemCount && atomic.LoadInt64(&enqueueCount) >= itemCount {
						break
					}
					runtime.Gosched()
				}
			}
		}()
	}

	// Start batch consumers
	for range batcherCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				// Random batch size between 2 and maxBatchSize
				batchSize := 2 + r.Intn(maxBatchSize-1)

				var items []string
				ok := q.TryDequeueBatch(batchSize, func(item string) { items = append(items, item) })
				if ok {
					atomic.AddInt64(&batchCount, int64(len(items)))
				} else {
					// Check if we're done
					total := atomic.LoadInt64(&dequeueCount) + atomic.LoadInt64(&batchCount)
					if total >= itemCount && atomic.LoadInt64(&enqueueCount) >= itemCount {
						break
					}
					runtime.Gosched()
				}
			}
		}()
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Verify all items were processed
	total := atomic.LoadInt64(&dequeueCount) + atomic.LoadInt64(&batchCount)
	expected := atomic.LoadInt64(&enqueueCount)

	for {
		_, ok := q.TryDequeue()
		if !ok {
			break
		}
		expected--
	}

	if total != expected {
		t.Fatalf("Expected %d dequeued items, got %d (single: %d, batch: %d)",
			expected, total, dequeueCount, batchCount)
	}
}

func TestQueueTryDequeueBatch_HighContention(t *testing.T) {
	const (
		queueSize      = 10    // Very small queue to maximize contention
		itemCount      = 50000 // Large number of items
		goroutineCount = 20    // Many goroutines fighting for access
		duration       = 2     // Test duration in seconds
	)

	q := newQueue[string](queueSize)

	var (
		enqueueCount int64
		dequeueCount int64
		batchCount   int64
		running      int32 = 1
	)

	var wg sync.WaitGroup

	// Start producer/consumer goroutines
	for i := range goroutineCount {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for atomic.LoadInt32(&running) == 1 {
				// Randomly choose between operations
				op := r.Intn(3)

				switch op {
				case 0: // Enqueue
					if atomic.LoadInt64(&enqueueCount) < itemCount {
						if q.TryEnqueue(fmt.Sprintf("item-%d", atomic.LoadInt64(&enqueueCount))) {
							// Successfully enqueued
							atomic.AddInt64(&enqueueCount, 1)
						}
					}
				case 1: // Dequeue
					if _, ok := q.TryDequeue(); ok {
						atomic.AddInt64(&dequeueCount, 1)
					}
				case 2: // Batch dequeue
					batchSize := 1 + r.Intn(5) // Random batch size 1-5
					var items []string
					if ok := q.TryDequeueBatch(batchSize, func(item string) { items = append(items, item) }); ok {
						atomic.AddInt64(&batchCount, int64(len(items)))
					}
				}

				// Occasionally yield to increase contention
				if r.Intn(100) < 5 {
					runtime.Gosched()
				}
			}
		}(i)
	}

	// Run the test for the specified duration
	time.Sleep(time.Duration(duration) * time.Second)
	atomic.StoreInt32(&running, 0)

	// Wait for all goroutines to finish
	wg.Wait()

	// Verify no items were lost
	total := atomic.LoadInt64(&dequeueCount) + atomic.LoadInt64(&batchCount)
	enqueued := atomic.LoadInt64(&enqueueCount)

	t.Logf("Enqueued: %d, Dequeued: %d (single: %d, batch: %d)",
		enqueued, total, dequeueCount, batchCount)

	// Check queue is empty or contains only the expected number of remaining items
	remaining := enqueued - total
	count := 0
	for {
		_, ok := q.TryDequeue()
		if !ok {
			break
		}
		count++
	}

	if int64(count) != remaining {
		t.Fatalf("Expected %d remaining items in queue, found %d", remaining, count)
	}
}

func TestQueueTryDequeueBatch_Stress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	const (
		queueSize      = 256
		operationCount = 10_000_000
		goroutineCount = 32
		maxBatchSize   = 16
	)

	q := newQueue[string](queueSize)

	var (
		enqueueCount  int64
		dequeueCount  int64
		batchCount    int64
		totalDequeued int64
		opsComplete   int64
		startWg       sync.WaitGroup
		doneWg        sync.WaitGroup
	)

	// Prepare goroutines but don't start them yet
	startWg.Add(1)

	// Launch worker goroutines
	for i := range goroutineCount {
		doneWg.Add(1)
		go func(id int) {
			defer doneWg.Done()

			// Wait for start signal
			startWg.Wait()

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			localOps := 0

			for {
				// Check if we've completed all operations
				if atomic.LoadInt64(&opsComplete) >= operationCount {
					break
				}

				// Randomly choose between operations
				op := r.Intn(10)

				if op < 5 { // 50% chance to enqueue
					item := fmt.Sprintf("item-%d-%d", id, atomic.LoadInt64(&enqueueCount)+1)
					if q.TryEnqueue(item) {
						atomic.AddInt64(&enqueueCount, 1)
						atomic.AddInt64(&opsComplete, 1)
						localOps++
					}
				} else if op < 8 { // 30% chance to dequeue
					if _, ok := q.TryDequeue(); ok {
						atomic.AddInt64(&dequeueCount, 1)
						atomic.AddInt64(&totalDequeued, 1)
						atomic.AddInt64(&opsComplete, 1)
						localOps++
					}
				} else { // 20% chance to batch dequeue
					batchSize := 1 + r.Intn(maxBatchSize)
					var items []string
					if ok := q.TryDequeueBatch(batchSize, func(item string) { items = append(items, item) }); ok {
						itemCount := int64(len(items))
						atomic.AddInt64(&batchCount, 1)
						atomic.AddInt64(&totalDequeued, itemCount)
						atomic.AddInt64(&opsComplete, 1)
						localOps++
					}
				}

				// Occasional yield to prevent CPU hogging
				if localOps%1000 == 0 {
					runtime.Gosched()
				}
			}
		}(i)
	}

	// Start timing
	t.Logf("Starting stress test with %d goroutines", goroutineCount)
	start := time.Now()

	// Start all goroutines simultaneously
	startWg.Done()

	// Wait for test to complete
	doneWg.Wait()

	// Print statistics
	elapsed := time.Since(start)
	opsPerSec := float64(operationCount) / elapsed.Seconds()

	t.Logf("Stress test completed in %v", elapsed)
	t.Logf("Operations per second: %.2f", opsPerSec)
	t.Logf("Enqueue operations: %d", atomic.LoadInt64(&enqueueCount))
	t.Logf("Dequeue operations: %d", atomic.LoadInt64(&dequeueCount))
	t.Logf("Batch dequeue operations: %d", atomic.LoadInt64(&batchCount))
	t.Logf("Total items dequeued: %d", atomic.LoadInt64(&totalDequeued))

	// Drain the queue and verify no items are lost
	remainingItems := 0
	for {
		_, ok := q.TryDequeue()
		if !ok {
			break
		}
		remainingItems++
	}

	t.Logf("Remaining items in queue: %d", remainingItems)
	expected := atomic.LoadInt64(&enqueueCount) - atomic.LoadInt64(&totalDequeued)
	if int64(remainingItems) != expected {
		t.Fatalf("Expected %d remaining items, found %d", expected, remainingItems)
	}
}

func TestQueueTryDequeueBatch_ExactCapacity(t *testing.T) {
	const capacity = 16

	q := newQueue[int](capacity)

	// Fill the queue exactly to capacity
	for i := range capacity {
		if !q.TryEnqueue(i) {
			t.Fatalf("Failed to enqueue item %d", i)
		}
	}

	// Try to dequeue exactly capacity items
	var items []int
	ok := q.TryDequeueBatch(capacity, func(item int) { items = append(items, item) })
	if !ok {
		t.Fatal("Failed to dequeue batch of exact capacity")
	}

	if len(items) != capacity {
		t.Fatalf("Expected %d items, got %d", capacity, len(items))
	}

	// Queue should be empty now
	_, ok = q.TryDequeue()
	if ok {
		t.Fatal("Expected queue to be empty")
	}

	// Try to dequeue a batch from the empty queue
	ok = q.TryDequeueBatch(1, func(item int) {})
	if ok {
		t.Fatal("Expected batch dequeue to fail on empty queue")
	}
}

func TestQueueTryDequeueBatch_MultipleWrap(t *testing.T) {
	const (
		capacity   = 4
		iterations = 10
	)

	q := newQueue[int](capacity)

	// Run multiple iterations to ensure wrapping
	for iter := range iterations {
		// Fill queue
		for i := range capacity {
			val := iter*capacity + i
			if !q.TryEnqueue(val) {
				t.Fatalf("Failed to enqueue item %d in iteration %d", i, iter)
			}
		}

		// Dequeue as batch
		var items []int
		ok := q.TryDequeueBatch(capacity, func(item int) { items = append(items, item) })
		if !ok {
			t.Fatalf("Failed to dequeue batch in iteration %d", iter)
		}

		// Verify values
		for i, item := range items {
			expected := iter*capacity + i
			if item != expected {
				t.Fatalf("Expected item %d, got %d in iteration %d", expected, item, iter)
			}
		}
	}

	// Verify we can still use the queue after multiple wrap-arounds
	if !q.TryEnqueue(999) {
		t.Fatal("Failed to enqueue after multiple wrap-arounds")
	}

	item, ok := q.TryDequeue()
	if !ok || item != 999 {
		t.Fatalf("Failed to dequeue after multiple wrap-arounds, got %v (ok=%v)", item, ok)
	}
}
