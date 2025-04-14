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
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/notfilippo/otto"
)

func TestMain(m *testing.M) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	m.Run()
}

type zipfs struct {
	s    float64
	v    float64
	name string
}

func TestHitRatio(t *testing.T) {
	if testing.Short() {
		t.Skip("hit ratio test takes a long time to run")
	} else {
		t.Log("hit ratio test started. the test can take a lot of time to run, use `go test --short` to skip it")
	}

	sizes := []int{100, 500, 1000, 5000}
	concurrency := []int{64, 1, 2, 4, 8, 16, 32}

	keySpace := uint64(10000)

	// Zipf parameters
	// s=1.01 is minimally skewed, s=2.0 is highly skewed
	distributions := []zipfs{
		{1.1, 1.0, "low skew"},
		{1.5, 1.0, "medium skew"},
		{2.0, 1.0, "high skew"},
	}

	// Ops per worker.
	ops := 500

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			for _, concurrency := range concurrency {
				t.Run(fmt.Sprintf("concurrency-%d", concurrency), func(t *testing.T) {
					for _, zipf := range distributions {
						t.Run(fmt.Sprintf("distribution-%s", zipf.name), func(t *testing.T) {
							c := otto.New(1<<10, size)
							hits, misses := run(c, keySpace, zipf, concurrency, ops)
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
	var popularityShift atomic.Uint64
	go func() {
		currentShift := uint64(0)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for range ops / 10000 {
			<-ticker.C
			currentShift = (currentShift + 1000) % keySpace
			popularityShift.Store(currentShift)
		}
	}()

	var (
		wg           sync.WaitGroup
		hits, misses atomic.Uint64
	)

	done := make(chan struct{})

	for id := range concurrency {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			source := rand.NewPCG(uint64(time.Now().UnixNano()), uint64(id))
			r := rand.New(source)
			d := rand.NewZipf(r, zipf.s, zipf.v, keySpace-1)

			burstMode := false

			var data []byte

			for range ops {
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
					keyRank = (keyRank + uint64(id*100)) % keySpace
				}

				key := fmt.Sprintf("key-%d", keyRank)

				if r.Float64() < 0.85 {
					value := c.Get(key, data)
					if len(value) == 0 || string(value) != key {
						misses.Add(1)

						// Simulate backend retrieval
						time.Sleep(time.Millisecond * time.Duration(5+r.IntN(20)))
						c.Set(key, []byte(key))
					} else {
						hits.Add(1)
					}
				} else {
					c.Set(key, []byte(key))
				}
			}
		}(id)
	}

	wg.Wait()
	close(done)
	return hits.Load(), misses.Load()
}
