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

// Modified version of https://github.com/puzpuzpuz/xsync/blob/main/rbmutex.go
// Licensed under Apache-2.0 Copyright 2025 Andrei Pechkurov

package otto

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// slow-down guard
const nslowdown = 7

// pool for reader tokens
var rtokenPool sync.Pool

// rToken is a reader lock token.
type rToken struct {
	slot uint32
	//lint:ignore U1000 prevents false sharing
	pad [cacheLineSize - 4]byte
}

// A rbMutex is a reader biased reader/writer mutual exclusion lock.
// The lock can be held by an many readers or a single writer.
// The zero value for a rbMutex is an unlocked mutex.
//
// A rbMutex must not be copied after first use.
//
// rbMutex is based on a modified version of BRAVO
// (Biased Locking for Reader-Writer Locks) algorithm:
// https://arxiv.org/pdf/1810.01553.pdf
//
// rbMutex is a specialized mutex for scenarios, such as caches,
// where the vast majority of locks are acquired by readers and write
// lock acquire attempts are infrequent. In such scenarios, rbMutex
// performs better than sync.RWMutex on large multicore machines.
//
// rbMutex extends sync.RWMutex internally and uses it as the "reader
// bias disabled" fallback, so the same semantics apply. The only
// noticeable difference is in reader tokens returned from the
// RLock/RUnlock methods.
type rbMutex struct {
	rslots       []rslot
	rmask        uint32
	rbias        int32
	inhibitUntil time.Time
	rw           sync.RWMutex
}

type rslot struct {
	mu int32
	//lint:ignore U1000 prevents false sharing
	pad [cacheLineSize - 4]byte
}

// newRBMutex creates a new RBMutex instance.
func newRBMutex() *rbMutex {
	nslots := nextPowOf2(parallelism())
	mu := rbMutex{
		rslots: make([]rslot, nslots),
		rmask:  nslots - 1,
		rbias:  1,
	}
	return &mu
}

// TryRLock tries to lock m for reading without blocking.
// When TryRLock succeeds, it returns true and a reader token.
// In case of a failure, a false is returned.
func (mu *rbMutex) TryRLock() (bool, *rToken) {
	if t := mu.fastRlock(); t != nil {
		return true, t
	}
	// Optimistic slow path.
	if mu.rw.TryRLock() {
		if atomic.LoadInt32(&mu.rbias) == 0 && time.Now().After(mu.inhibitUntil) {
			atomic.StoreInt32(&mu.rbias, 1)
		}
		return true, nil
	}
	return false, nil
}

// RLock locks m for reading and returns a reader token. The
// token must be used in the later RUnlock call.
//
// Should not be used for recursive read locking; a blocked Lock
// call excludes new readers from acquiring the lock.
func (mu *rbMutex) RLock() *rToken {
	if t := mu.fastRlock(); t != nil {
		return t
	}
	// Slow path.
	mu.rw.RLock()
	if atomic.LoadInt32(&mu.rbias) == 0 && time.Now().After(mu.inhibitUntil) {
		atomic.StoreInt32(&mu.rbias, 1)
	}
	return nil
}

func (mu *rbMutex) fastRlock() *rToken {
	if atomic.LoadInt32(&mu.rbias) == 1 {
		t, ok := rtokenPool.Get().(*rToken)
		if !ok {
			t = new(rToken)
			t.slot = runtime_fastrand()
		}
		// Try all available slots to distribute reader threads to slots.
		for i := range mu.rslots {
			slot := t.slot + uint32(i)
			rslot := &mu.rslots[slot&mu.rmask]
			rslotmu := atomic.LoadInt32(&rslot.mu)
			if atomic.CompareAndSwapInt32(&rslot.mu, rslotmu, rslotmu+1) {
				if atomic.LoadInt32(&mu.rbias) == 1 {
					// Hot path succeeded.
					t.slot = slot
					return t
				}
				// The mutex is no longer reader biased. Roll back.
				atomic.AddInt32(&rslot.mu, -1)
				rtokenPool.Put(t)
				return nil
			}
			// Contention detected. Give a try with the next slot.
		}
	}
	return nil
}

// RUnlock undoes a single RLock call. A reader token obtained from
// the RLock call must be provided. RUnlock does not affect other
// simultaneous readers. A panic is raised if m is not locked for
// reading on entry to RUnlock.
func (mu *rbMutex) RUnlock(t *rToken) {
	if t == nil {
		mu.rw.RUnlock()
		return
	}
	if atomic.AddInt32(&mu.rslots[t.slot&mu.rmask].mu, -1) < 0 {
		panic("invalid reader state detected")
	}
	rtokenPool.Put(t)
}

// TryLock tries to lock m for writing without blocking.
func (mu *rbMutex) TryLock() bool {
	if mu.rw.TryLock() {
		if atomic.LoadInt32(&mu.rbias) == 1 {
			atomic.StoreInt32(&mu.rbias, 0)
			for i := range mu.rslots {
				if atomic.LoadInt32(&mu.rslots[i].mu) > 0 {
					// There is a reader. Roll back.
					atomic.StoreInt32(&mu.rbias, 1)
					mu.rw.Unlock()
					return false
				}
			}
		}
		return true
	}
	return false
}

// Lock locks m for writing. If the lock is already locked for
// reading or writing, Lock blocks until the lock is available.
func (mu *rbMutex) Lock() {
	mu.rw.Lock()
	if atomic.LoadInt32(&mu.rbias) == 1 {
		atomic.StoreInt32(&mu.rbias, 0)
		start := time.Now()
		for i := range mu.rslots {
			for atomic.LoadInt32(&mu.rslots[i].mu) > 0 {
				runtime.Gosched()
			}
		}
		mu.inhibitUntil = time.Now().Add(time.Since(start) * nslowdown)
	}
}

// Unlock unlocks m for writing. A panic is raised if m is not locked
// for writing on entry to Unlock.
//
// As with RWMutex, a locked RBMutex is not associated with a
// particular goroutine. One goroutine may RLock (Lock) a RBMutex and
// then arrange for another goroutine to RUnlock (Unlock) it.
func (mu *rbMutex) Unlock() {
	mu.rw.Unlock()
}
