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
	"fmt"
	"hash/maphash"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

type mapResizeHint int

const (
	mapGrowHint   mapResizeHint = 0
	mapShrinkHint mapResizeHint = 1
	mapClearHint  mapResizeHint = 2
)

const (
	// number of Map entries per bucket; 3 entries lead to size of 64B
	// (one cache line) on 64-bit machines
	entriesPerMapBucket = 3
	// threshold fraction of table occupation to start a table shrinking
	// when deleting the last entry in a bucket chain
	mapShrinkFraction = 128
	// map load factor to trigger a table resize during insertion;
	// a map holds up to mapLoadFactor*entriesPerMapBucket*mapTableLen
	// key-value pairs (this is a soft limit)
	mapLoadFactor = 0.75
	// minimal table size, i.e. number of buckets; thus, minimal map
	// capacity can be calculated as entriesPerMapBucket*defaultMinMapTableLen
	defaultMinMapTableLen = 32
	// minimum counter stripes to use
	minMapCounterLen = 8
	// maximum counter stripes to use; stands for around 4KB of memory
	maxMapCounterLen = 32
)

var (
	topHashMask       = uint64((1<<20)-1) << 44
	topHashEntryMasks = [3]uint64{
		topHashMask,
		topHashMask >> 20,
		topHashMask >> 40,
	}
)

// hmap is like a Go map[string]*entry but is safe for concurrent
// use by multiple goroutines without additional locking or
// coordination. It follows the interface of sync.hmap with
// a number of valuable extensions like Compute or Size.
//
// A hmap must not be copied after first use.
//
// hmap uses a modified version of Cache-Line Hash Table (CLHT)
// data structure: https://github.com/LPD-EPFL/CLHT
//
// CLHT is built around idea to organize the hash table in
// cache-line-sized buckets, so that on all modern CPUs update
// operations complete with at most one cache-line transfer.
// Also, Get operations involve no write to memory, as well as no
// mutexes or any other sort of locks. Due to this design, in all
// considered scenarios hmap outperforms sync.hmap.
//
// One important difference with sync.hmap is that only string keys
// are supported. That's because Golang standard library does not
// expose the built-in hash functions for *entry values.
type hmap struct {
	totalGrowths int64
	totalShrinks int64
	resizing     int64          // resize in progress flag; updated atomically
	resizeMu     sync.Mutex     // only used along with resizeCond
	resizeCond   sync.Cond      // used to wake up resize waiters (concurrent modifications)
	table        unsafe.Pointer // *mapTable
	minTableLen  int
	growOnly     bool
}

type mapTable struct {
	buckets []bucketPadded
	// striped counter for number of table entries;
	// used to determine if a table shrinking is needed
	// occupies min(buckets_memory/1024, 64KB) of memory
	size []counterStripe
	seed maphash.Seed
}

type counterStripe struct {
	c int64
	//lint:ignore U1000 prevents false sharing
	pad [cacheLineSize - 8]byte
}

type bucketPadded struct {
	//lint:ignore U1000 ensure each bucket takes two cache lines on both 32 and 64-bit archs
	pad [cacheLineSize - unsafe.Sizeof(bucket{})]byte
	bucket
}

type bucket struct {
	next   unsafe.Pointer // *bucketPadded
	keys   [entriesPerMapBucket]uint64
	values [entriesPerMapBucket]unsafe.Pointer
	// topHashMutex is a 2-in-1 value.
	//
	// It contains packed top 20 bits (20 MSBs) of hash codes for keys
	// stored in the bucket:
	// | key 0's top hash | key 1's top hash | key 2's top hash | bitmap for keys | mutex |
	// |      20 bits     |      20 bits     |      20 bits     |     3 bits      | 1 bit |
	//
	// The least significant bit is used for the mutex (TTAS spinlock).
	topHashMutex uint64
}

type rangeEntry struct {
	key   uint64
	value unsafe.Pointer
}

// mapConfig defines configurable Map/MapOf options.
type mapConfig struct {
	sizeHint int
	growOnly bool
}

// withPresize configures new Map/MapOf instance with capacity enough
// to hold sizeHint entries. The capacity is treated as the minimal
// capacity meaning that the underlying hash table will never shrink
// to a smaller capacity. If sizeHint is zero or negative, the value
// is ignored.
func withPresize(sizeHint int) func(*mapConfig) {
	return func(c *mapConfig) {
		c.sizeHint = sizeHint
	}
}

// withGrowOnly configures new Map/MapOf instance to be grow-only.
// This means that the underlying hash table grows in capacity when
// new keys are added, but does not shrink when keys are deleted.
// The only exception to this rule is the Clear method which
// shrinks the hash table back to the initial capacity.
func withGrowOnly() func(*mapConfig) {
	return func(c *mapConfig) {
		c.growOnly = true
	}
}

// newMap creates a new Map instance configured with the given
// options.
func newMap(options ...func(*mapConfig)) *hmap {
	c := &mapConfig{
		sizeHint: defaultMinMapTableLen * entriesPerMapBucket,
	}
	for _, o := range options {
		o(c)
	}

	m := &hmap{}
	m.resizeCond = *sync.NewCond(&m.resizeMu)
	var table *mapTable
	if c.sizeHint <= defaultMinMapTableLen*entriesPerMapBucket {
		table = newMapTable(defaultMinMapTableLen)
	} else {
		tableLen := nextPowOf2(uint32((float64(c.sizeHint) / entriesPerMapBucket) / mapLoadFactor))
		table = newMapTable(int(tableLen))
	}
	m.minTableLen = len(table.buckets)
	m.growOnly = c.growOnly
	atomic.StorePointer(&m.table, unsafe.Pointer(table))
	return m
}

// newMapPresized creates a new Map instance with capacity enough to hold
// sizeHint entries. The capacity is treated as the minimal capacity
// meaning that the underlying hash table will never shrink to
// a smaller capacity. If sizeHint is zero or negative, the value
// is ignored.
//
// Deprecated: use NewMap in combination with WithPresize.
func newMapPresized(sizeHint int) *hmap {
	return newMap(withPresize(sizeHint))
}

func newMapTable(minTableLen int) *mapTable {
	buckets := make([]bucketPadded, minTableLen)
	counterLen := minTableLen >> 10
	if counterLen < minMapCounterLen {
		counterLen = minMapCounterLen
	} else if counterLen > maxMapCounterLen {
		counterLen = maxMapCounterLen
	}
	counter := make([]counterStripe, counterLen)
	t := &mapTable{
		buckets: buckets,
		size:    counter,
		seed:    maphash.MakeSeed(),
	}
	return t
}

// toPlainMap returns a native map with a copy of xsync Map's
// contents. The copied xsync Map should not be modified while
// this call is made. If the copied Map is modified, the copying
// behavior is the same as in the Range method.
func toPlainMap(m *hmap) map[uint64]*entryHeader {
	pm := make(map[uint64]*entryHeader)
	if m != nil {
		m.Range(func(key uint64, value *entryHeader) bool {
			pm[key] = value
			return true
		})
	}
	return pm
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *hmap) Load(hash uint64) (value *entryHeader, ok bool) {
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	bidx := uint64(len(table.buckets)-1) & hash
	b := &table.buckets[bidx]
	for {
		topHashes := atomic.LoadUint64(&b.topHashMutex)
		for i := range entriesPerMapBucket {
			if !topHashMatch(hash, topHashes, i) {
				continue
			}
		atomic_snapshot:
			// Start atomic snapshot.
			vp := atomic.LoadPointer(&b.values[i])
			kp := atomic.LoadUint64(&b.keys[i])
			if vp != nil {
				if hash == derefKey(kp) {
					if uintptr(vp) == uintptr(atomic.LoadPointer(&b.values[i])) {
						// Atomic snapshot succeeded.
						return derefValue(vp), true
					}
					// Concurrent update/remove. Go for another spin.
					goto atomic_snapshot
				}
			}
		}
		bptr := atomic.LoadPointer(&b.next)
		if bptr == nil {
			return
		}
		b = (*bucketPadded)(bptr)
	}
}

// Store sets the value for a key.
func (m *hmap) Store(key uint64, value *entryHeader) {
	m.doCompute(
		key,
		func(*entryHeader, bool) (*entryHeader, bool) {
			return value, false
		},
		false,
		false,
	)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *hmap) LoadOrStore(key uint64, value *entryHeader) (actual *entryHeader, loaded bool) {
	return m.doCompute(
		key,
		func(*entryHeader, bool) (*entryHeader, bool) {
			return value, false
		},
		true,
		false,
	)
}

// LoadAndStore returns the existing value for the key if present,
// while setting the new value for the key.
// It stores the new value and returns the existing one, if present.
// The loaded result is true if the existing value was loaded,
// false otherwise.
func (m *hmap) LoadAndStore(key uint64, value *entryHeader) (actual *entryHeader, loaded bool) {
	return m.doCompute(
		key,
		func(*entryHeader, bool) (*entryHeader, bool) {
			return value, false
		},
		false,
		false,
	)
}

// LoadOrCompute returns the existing value for the key if present.
// Otherwise, it computes the value using the provided function, and
// then stores and returns the computed value. The loaded result is
// true if the value was loaded, false if computed.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (m *hmap) LoadOrCompute(key uint64, valueFn func() *entryHeader) (actual *entryHeader, loaded bool) {
	return m.doCompute(
		key,
		func(*entryHeader, bool) (*entryHeader, bool) {
			return valueFn(), false
		},
		true,
		false,
	)
}

// LoadOrTryCompute returns the existing value for the key if present.
// Otherwise, it tries to compute the value using the provided function
// and, if successful, stores and returns the computed value. The loaded
// result is true if the value was loaded, or false if computed (whether
// successfully or not). If the compute attempt was cancelled (due to an
// error, for example), a nil value will be returned.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (m *hmap) LoadOrTryCompute(
	key uint64,
	valueFn func() (newValue *entryHeader, cancel bool),
) (value *entryHeader, loaded bool) {
	return m.doCompute(
		key,
		func(*entryHeader, bool) (*entryHeader, bool) {
			nv, c := valueFn()
			if !c {
				return nv, false
			}
			return nil, true
		},
		true,
		false,
	)
}

// Compute either sets the computed new value for the key or deletes
// the value for the key. When the delete result of the valueFn function
// is set to true, the value will be deleted, if it exists. When delete
// is set to false, the value is updated to the newValue.
// The ok result indicates whether value was computed and stored, thus, is
// present in the map. The actual result contains the new value in cases where
// the value was computed and stored. See the example for a few use cases.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (m *hmap) Compute(
	key uint64,
	valueFn func(oldValue *entryHeader, loaded bool) (newValue *entryHeader, delete bool),
) (actual *entryHeader, ok bool) {
	return m.doCompute(key, valueFn, false, true)
}

// LoadAndDelete deletes the value for a key, returning the previous
// value if any. The loaded result reports whether the key was
// present.
func (m *hmap) LoadAndDelete(key uint64) (value *entryHeader, loaded bool) {
	return m.doCompute(
		key,
		func(value *entryHeader, loaded bool) (*entryHeader, bool) {
			return value, true
		},
		false,
		false,
	)
}

// Delete deletes the value for a key.
func (m *hmap) Delete(key uint64) {
	m.doCompute(
		key,
		func(value *entryHeader, loaded bool) (*entryHeader, bool) {
			return value, true
		},
		false,
		false,
	)
}

func (m *hmap) doCompute(
	key uint64,
	valueFn func(oldValue *entryHeader, loaded bool) (*entryHeader, bool),
	loadIfExists, computeOnly bool,
) (*entryHeader, bool) {
	// Read-only path.
	if loadIfExists {
		if v, ok := m.Load(key); ok {
			return v, !computeOnly
		}
	}
	// Write path.
	for {
	compute_attempt:
		var (
			emptyb       *bucketPadded
			emptyidx     int
			hintNonEmpty int
		)
		table := (*mapTable)(atomic.LoadPointer(&m.table))
		tableLen := len(table.buckets)
		bidx := uint64(len(table.buckets)-1) & key
		rootb := &table.buckets[bidx]
		lockBucket(&rootb.topHashMutex)
		// The following two checks must go in reverse to what's
		// in the resize method.
		if m.resizeInProgress() {
			// Resize is in progress. Wait, then go for another attempt.
			unlockBucket(&rootb.topHashMutex)
			m.waitForResize()
			goto compute_attempt
		}
		if m.newerTableExists(table) {
			// Someone resized the table. Go for another attempt.
			unlockBucket(&rootb.topHashMutex)
			goto compute_attempt
		}
		b := rootb
		for {
			topHashes := atomic.LoadUint64(&b.topHashMutex)
			for i := range entriesPerMapBucket {
				if b.keys[i] == 0 {
					if emptyb == nil {
						emptyb = b
						emptyidx = i
					}
					continue
				}
				if !topHashMatch(key, topHashes, i) {
					hintNonEmpty++
					continue
				}
				if key == derefKey(b.keys[i]) {
					vp := b.values[i]
					if loadIfExists {
						unlockBucket(&rootb.topHashMutex)
						return derefValue(vp), !computeOnly
					}
					// In-place update/delete.
					// We get a copy of the value via an *entry on each call,
					// thus the live value pointers are unique. Otherwise atomic
					// snapshot won't be correct in case of multiple Store calls
					// using the same value.
					oldValue := derefValue(vp)
					newValue, del := valueFn(oldValue, true)
					if del {
						// Deletion.
						// First we update the value, then the key.
						// This is important for atomic snapshot states.
						atomic.StoreUint64(&b.topHashMutex, eraseTopHash(topHashes, i))
						atomic.StorePointer(&b.values[i], nil)
						atomic.StoreUint64(&b.keys[i], 0)
						leftEmpty := false
						if hintNonEmpty == 0 {
							leftEmpty = isEmptyBucket(b)
						}
						unlockBucket(&rootb.topHashMutex)
						table.addSize(bidx, -1)
						// Might need to shrink the table.
						if leftEmpty {
							m.resize(table, mapShrinkHint)
						}
						return oldValue, !computeOnly
					}
					nvp := unsafe.Pointer(newValue)
					atomic.StorePointer(&b.values[i], nvp)
					unlockBucket(&rootb.topHashMutex)
					if computeOnly {
						// Compute expects the new value to be returned.
						return newValue, true
					}
					// LoadAndStore expects the old value to be returned.
					return oldValue, true
				}
				hintNonEmpty++
			}
			if b.next == nil {
				if emptyb != nil {
					// Insertion into an existing bucket.
					var zeroV *entryHeader
					newValue, del := valueFn(zeroV, false)
					if del {
						unlockBucket(&rootb.topHashMutex)
						return zeroV, false
					}
					// First we update the value, then the key.
					// This is important for atomic snapshot states.
					topHashes = atomic.LoadUint64(&emptyb.topHashMutex)
					atomic.StoreUint64(&emptyb.topHashMutex, storeTopHash(key, topHashes, emptyidx))
					atomic.StorePointer(&emptyb.values[emptyidx], unsafe.Pointer(newValue))
					atomic.StoreUint64(&emptyb.keys[emptyidx], key)
					unlockBucket(&rootb.topHashMutex)
					table.addSize(bidx, 1)
					return newValue, computeOnly
				}
				growThreshold := float64(tableLen) * entriesPerMapBucket * mapLoadFactor
				if table.sumSize() > int64(growThreshold) {
					// Need to grow the table. Then go for another attempt.
					unlockBucket(&rootb.topHashMutex)
					m.resize(table, mapGrowHint)
					goto compute_attempt
				}
				// Insertion into a new bucket.
				var zeroV *entryHeader
				newValue, del := valueFn(zeroV, false)
				if del {
					unlockBucket(&rootb.topHashMutex)
					return newValue, false
				}
				// Create and append a bucket.
				newb := new(bucketPadded)
				newb.keys[0] = key
				newb.values[0] = unsafe.Pointer(newValue)
				newb.topHashMutex = storeTopHash(key, newb.topHashMutex, 0)
				atomic.StorePointer(&b.next, unsafe.Pointer(newb))
				unlockBucket(&rootb.topHashMutex)
				table.addSize(bidx, 1)
				return newValue, computeOnly
			}
			b = (*bucketPadded)(b.next)
		}
	}
}

func (m *hmap) newerTableExists(table *mapTable) bool {
	curTablePtr := atomic.LoadPointer(&m.table)
	return uintptr(curTablePtr) != uintptr(unsafe.Pointer(table))
}

func (m *hmap) resizeInProgress() bool {
	return atomic.LoadInt64(&m.resizing) == 1
}

func (m *hmap) waitForResize() {
	m.resizeMu.Lock()
	for m.resizeInProgress() {
		m.resizeCond.Wait()
	}
	m.resizeMu.Unlock()
}

func (m *hmap) resize(knownTable *mapTable, hint mapResizeHint) {
	knownTableLen := len(knownTable.buckets)
	// Fast path for shrink attempts.
	if hint == mapShrinkHint {
		if m.growOnly ||
			m.minTableLen == knownTableLen ||
			knownTable.sumSize() > int64((knownTableLen*entriesPerMapBucket)/mapShrinkFraction) {
			return
		}
	}
	// Slow path.
	if !atomic.CompareAndSwapInt64(&m.resizing, 0, 1) {
		// Someone else started resize. Wait for it to finish.
		m.waitForResize()
		return
	}
	var newTable *mapTable
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	tableLen := len(table.buckets)
	switch hint {
	case mapGrowHint:
		// Grow the table with factor of 2.
		atomic.AddInt64(&m.totalGrowths, 1)
		newTable = newMapTable(tableLen << 1)
	case mapShrinkHint:
		shrinkThreshold := int64((tableLen * entriesPerMapBucket) / mapShrinkFraction)
		if tableLen > m.minTableLen && table.sumSize() <= shrinkThreshold {
			// Shrink the table with factor of 2.
			atomic.AddInt64(&m.totalShrinks, 1)
			newTable = newMapTable(tableLen >> 1)
		} else {
			// No need to shrink. Wake up all waiters and give up.
			m.resizeMu.Lock()
			atomic.StoreInt64(&m.resizing, 0)
			m.resizeCond.Broadcast()
			m.resizeMu.Unlock()
			return
		}
	case mapClearHint:
		newTable = newMapTable(m.minTableLen)
	default:
		panic(fmt.Sprintf("unexpected resize hint: %d", hint))
	}
	// Copy the data only if we're not clearing the map.
	if hint != mapClearHint {
		for i := range tableLen {
			copied := copyBucket(&table.buckets[i], newTable)
			newTable.addSizePlain(uint64(i), copied)
		}
	}
	// Publish the new table and wake up all waiters.
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	m.resizeMu.Lock()
	atomic.StoreInt64(&m.resizing, 0)
	m.resizeCond.Broadcast()
	m.resizeMu.Unlock()
}

func copyBucket(b *bucketPadded, destTable *mapTable) (copied int) {
	rootb := b
	lockBucket(&rootb.topHashMutex)
	for {
		for i := range entriesPerMapBucket {
			if b.keys[i] != 0 {
				hash := derefKey(b.keys[i])
				bidx := uint64(len(destTable.buckets)-1) & hash
				destb := &destTable.buckets[bidx]
				appendToBucket(hash, b.values[i], destb)
				copied++
			}
		}
		if b.next == nil {
			unlockBucket(&rootb.topHashMutex)
			return
		}
		b = (*bucketPadded)(b.next)
	}
}

func appendToBucket(hash uint64, valPtr unsafe.Pointer, b *bucketPadded) {
	for {
		for i := range entriesPerMapBucket {
			if b.keys[i] == 0 {
				b.keys[i] = hash
				b.values[i] = valPtr
				b.topHashMutex = storeTopHash(hash, b.topHashMutex, i)
				return
			}
		}
		if b.next == nil {
			newb := new(bucketPadded)
			newb.keys[0] = hash
			newb.values[0] = valPtr
			newb.topHashMutex = storeTopHash(hash, newb.topHashMutex, 0)
			b.next = unsafe.Pointer(newb)
			return
		}
		b = (*bucketPadded)(b.next)
	}
}

func isEmptyBucket(rootb *bucketPadded) bool {
	b := rootb
	for {
		for i := range entriesPerMapBucket {
			if b.keys[i] != 0 {
				return false
			}
		}
		if b.next == nil {
			return true
		}
		b = (*bucketPadded)(b.next)
	}
}

// Range calls f sequentially for each key and value present in the
// map. If f returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot
// of the Map's contents: no key will be visited more than once, but
// if the value for any key is stored or deleted concurrently, Range
// may reflect any mapping for that key from any point during the
// Range call.
//
// It is safe to modify the map while iterating it, including entry
// creation, modification and deletion. However, the concurrent
// modification rule apply, i.e. the changes may be not reflected
// in the subsequently iterated entries.
func (m *hmap) Range(f func(key uint64, value *entryHeader) bool) {
	var zeroEntry rangeEntry
	// Pre-allocate array big enough to fit entries for most hash tables.
	bentries := make([]rangeEntry, 0, 16*entriesPerMapBucket)
	tablep := atomic.LoadPointer(&m.table)
	table := *(*mapTable)(tablep)
	for i := range table.buckets {
		rootb := &table.buckets[i]
		b := rootb
		// Prevent concurrent modifications and copy all entries into
		// the intermediate slice.
		lockBucket(&rootb.topHashMutex)
		for {
			for i := range entriesPerMapBucket {
				if b.keys[i] != 0 {
					bentries = append(bentries, rangeEntry{
						key:   b.keys[i],
						value: b.values[i],
					})
				}
			}
			if b.next == nil {
				unlockBucket(&rootb.topHashMutex)
				break
			}
			b = (*bucketPadded)(b.next)
		}
		// Call the function for all copied entries.
		for j := range bentries {
			k := derefKey(bentries[j].key)
			v := derefValue(bentries[j].value)
			if !f(k, v) {
				return
			}
			// Remove the reference to avoid preventing the copied
			// entries from being GCed until this method finishes.
			bentries[j] = zeroEntry
		}
		bentries = bentries[:0]
	}
}

// Clear deletes all keys and values currently stored in the map.
func (m *hmap) Clear() {
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	m.resize(table, mapClearHint)
}

// Size returns current size of the map.
func (m *hmap) Size() int {
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	return int(table.sumSize())
}

func derefKey(keyPtr uint64) uint64 {
	return keyPtr
}

func derefValue(valuePtr unsafe.Pointer) *entryHeader {
	return (*entryHeader)(valuePtr)
}

func lockBucket(mu *uint64) {
	for {
		var v uint64
		for {
			v = atomic.LoadUint64(mu)
			if v&1 != 1 {
				break
			}
			runtime.Gosched()
		}
		if atomic.CompareAndSwapUint64(mu, v, v|1) {
			return
		}
		runtime.Gosched()
	}
}

func unlockBucket(mu *uint64) {
	v := atomic.LoadUint64(mu)
	atomic.StoreUint64(mu, v&^1)
}

func topHashMatch(hash, topHashes uint64, idx int) bool {
	if topHashes&(1<<(idx+1)) == 0 {
		// Entry is not present.
		return false
	}
	hash = hash & topHashMask
	topHashes = (topHashes & topHashEntryMasks[idx]) << (20 * idx)
	return hash == topHashes
}

func storeTopHash(hash, topHashes uint64, idx int) uint64 {
	// Zero out top hash at idx.
	topHashes = topHashes &^ topHashEntryMasks[idx]
	// Chop top 20 MSBs of the given hash and position them at idx.
	hash = (hash & topHashMask) >> (20 * idx)
	// Store the MSBs.
	topHashes = topHashes | hash
	// Mark the entry as present.
	return topHashes | (1 << (idx + 1))
}

func eraseTopHash(topHashes uint64, idx int) uint64 {
	return topHashes &^ (1 << (idx + 1))
}

func (table *mapTable) addSize(bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	atomic.AddInt64(&table.size[cidx].c, int64(delta))
}

func (table *mapTable) addSizePlain(bucketIdx uint64, delta int) {
	cidx := uint64(len(table.size)-1) & bucketIdx
	table.size[cidx].c += int64(delta)
}

func (table *mapTable) sumSize() int64 {
	sum := int64(0)
	for i := range table.size {
		sum += atomic.LoadInt64(&table.size[i].c)
	}
	return sum
}

// mapStats is Map/MapOf statistics.
//
// Warning: map statistics are intented to be used for diagnostic
// purposes, not for production code. This means that breaking changes
// may be introduced into this struct even between minor releases.
type mapStats struct {
	// RootBuckets is the number of root buckets in the hash table.
	// Each bucket holds a few entries.
	RootBuckets int
	// TotalBuckets is the total number of buckets in the hash table,
	// including root and their chained buckets. Each bucket holds
	// a few entries.
	TotalBuckets int
	// EmptyBuckets is the number of buckets that hold no entries.
	EmptyBuckets int
	// Capacity is the Map/MapOf capacity, i.e. the total number of
	// entries that all buckets can physically hold. This number
	// does not consider the load factor.
	Capacity int
	// Size is the exact number of entries stored in the map.
	Size int
	// Counter is the number of entries stored in the map according
	// to the internal atomic counter. In case of concurrent map
	// modifications this number may be different from Size.
	Counter int
	// CounterLen is the number of internal atomic counter stripes.
	// This number may grow with the map capacity to improve
	// multithreaded scalability.
	CounterLen int
	// MinEntries is the minimum number of entries per a chain of
	// buckets, i.e. a root bucket and its chained buckets.
	MinEntries int
	// MinEntries is the maximum number of entries per a chain of
	// buckets, i.e. a root bucket and its chained buckets.
	MaxEntries int
	// TotalGrowths is the number of times the hash table grew.
	TotalGrowths int64
	// TotalGrowths is the number of times the hash table shrinked.
	TotalShrinks int64
}

// ToString returns string representation of map stats.
func (s *mapStats) ToString() string {
	var sb strings.Builder
	sb.WriteString("MapStats{\n")
	sb.WriteString(fmt.Sprintf("RootBuckets:  %d\n", s.RootBuckets))
	sb.WriteString(fmt.Sprintf("TotalBuckets: %d\n", s.TotalBuckets))
	sb.WriteString(fmt.Sprintf("EmptyBuckets: %d\n", s.EmptyBuckets))
	sb.WriteString(fmt.Sprintf("Capacity:     %d\n", s.Capacity))
	sb.WriteString(fmt.Sprintf("Size:         %d\n", s.Size))
	sb.WriteString(fmt.Sprintf("Counter:      %d\n", s.Counter))
	sb.WriteString(fmt.Sprintf("CounterLen:   %d\n", s.CounterLen))
	sb.WriteString(fmt.Sprintf("MinEntries:   %d\n", s.MinEntries))
	sb.WriteString(fmt.Sprintf("MaxEntries:   %d\n", s.MaxEntries))
	sb.WriteString(fmt.Sprintf("TotalGrowths: %d\n", s.TotalGrowths))
	sb.WriteString(fmt.Sprintf("TotalShrinks: %d\n", s.TotalShrinks))
	sb.WriteString("}\n")
	return sb.String()
}

// Stats returns statistics for the Map. Just like other map
// methods, this one is thread-safe. Yet it's an O(N) operation,
// so it should be used only for diagnostics or debugging purposes.
func (m *hmap) Stats() mapStats {
	stats := mapStats{
		TotalGrowths: atomic.LoadInt64(&m.totalGrowths),
		TotalShrinks: atomic.LoadInt64(&m.totalShrinks),
		MinEntries:   math.MaxInt32,
	}
	table := (*mapTable)(atomic.LoadPointer(&m.table))
	stats.RootBuckets = len(table.buckets)
	stats.Counter = int(table.sumSize())
	stats.CounterLen = len(table.size)
	for i := range table.buckets {
		nentries := 0
		b := &table.buckets[i]
		stats.TotalBuckets++
		for {
			nentriesLocal := 0
			stats.Capacity += entriesPerMapBucket
			for i := range entriesPerMapBucket {
				if atomic.LoadUint64(&b.keys[i]) != 0 {
					stats.Size++
					nentriesLocal++
				}
			}
			nentries += nentriesLocal
			if nentriesLocal == 0 {
				stats.EmptyBuckets++
			}
			if b.next == nil {
				break
			}
			b = (*bucketPadded)(atomic.LoadPointer(&b.next))
			stats.TotalBuckets++
		}
		if nentries < stats.MinEntries {
			stats.MinEntries = nentries
		}
		if nentries > stats.MaxEntries {
			stats.MaxEntries = nentries
		}
	}
	return stats
}

// nextPowOf2 computes the next highest power of 2 of 32-bit v.
// Source: https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
func nextPowOf2(v uint32) uint32 {
	if v == 0 {
		return 1
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
