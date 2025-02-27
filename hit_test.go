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
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/notfilippo/otto"
)

const workloadMultiplier = 15

type zipfs struct {
	s    float64
	v    float64
	name string
}

func TestHitRatio(t *testing.T) {
	t.Skip("hit ratio test takes a long time to run")

	cacheSizes := []int{100, 500, 1000, 5000}
	workers := []int{32, 16, 8, 4, 2, 1}
	ops := 10_000

	keySpace := uint64(10000)

	// Zipf parameters
	// s=1.01 is minimally skewed, s=2.0 is highly skewed
	distributions := []zipfs{
		{1.1, 1.0, "low skew"},
		{1.5, 1.0, "medium skew"},
		{2.0, 1.0, "high skew"},
	}

	for _, cacheSize := range cacheSizes {
		t.Run(fmt.Sprintf("items-%d", cacheSize), func(t *testing.T) {
			for _, workerCount := range workers {
				t.Run(fmt.Sprintf("workers-%d", workerCount), func(t *testing.T) {
					for _, zipf := range distributions {
						t.Run(fmt.Sprintf("distribution-%s", zipf.name), func(t *testing.T) {
							c := otto.New(32, cacheSize)
							hits, misses := run(c, keySpace, zipf, workerCount, ops)
							c.Close()

							t.Logf("%f hit%%", float64(hits)/float64(hits+misses)*100.)
						})
					}
				})
			}
		})
	}
}

func run(c otto.Cache, keySpace uint64, zipf zipfs, concurrency int, ops int) (uint64, uint64) {
	opsPerWorker := ops / concurrency
	totalOps := opsPerWorker * concurrency

	var popularityShift atomic.Uint64
	go func() {
		currentShift := uint64(0)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for i := 0; i < ops/10000; i++ {
			<-ticker.C
			currentShift = (currentShift + 1000) % keySpace
			popularityShift.Store(currentShift)
		}
	}()

	var (
		wg           sync.WaitGroup
		hits, misses atomic.Uint64
		completedOps atomic.Uint64
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range time.Tick(500 * time.Millisecond) {
			fmt.Printf("\r%d/%d", completedOps.Load(), totalOps)
			if completedOps.Load() >= uint64(totalOps) {
				fmt.Println()
				break
			}
		}
	}()

	for workerId := 0; workerId < concurrency; workerId++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			source := rand.NewPCG(uint64(time.Now().UnixNano()), uint64(workerId))
			r := rand.New(source)
			d := rand.NewZipf(r, zipf.s, zipf.v, keySpace-1)

			burstMode := false

			for j := 0; j < opsPerWorker; j++ {

				// Occasionally switch between normal and burst modes
				if r.Float64() < 0.01 {
					burstMode = !burstMode
				}

				// Simulate different access speeds based on mode
				if burstMode {
					time.Sleep(time.Microsecond * time.Duration(r.IntN(50)))
				} else {
					time.Sleep(time.Millisecond * time.Duration(1+r.IntN(5)))
				}

				keyRank := (d.Uint64() + popularityShift.Load()) % keySpace

				if r.Float64() < 0.3 {
					keyRank = (keyRank + uint64(workerId*100)) % keySpace
				}

				key := fmt.Sprintf("key-%d", keyRank)

				if r.Float64() < 0.85 {
					item := c.Get(key, nil)
					if item == nil {
						misses.Add(1)

						// Simulate backend retrieval
						time.Sleep(time.Millisecond * time.Duration(5+r.IntN(20)))
						c.Set(key, []byte{0xAA})
					} else {
						hits.Add(1)
					}
				} else {
					c.Set(key, []byte{0xAA})
				}

				completedOps.Add(1)
			}
		}(workerId)
	}

	wg.Wait()

	return hits.Load(), misses.Load()
}
