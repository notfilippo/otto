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
	"testing"

	"github.com/notfilippo/otto"
)

const workloadMultiplier = 15

func BenchmarkHitRatio(b *testing.B) {
	zipfAlphas := []float64{0.99}
	items := []int{1e5 * 5}
	concurrencies := []int{1, 2, 4, 8, 16}
	cacheSizeMultiplier := []float64{0.001, 0.01, 0.1}

	b.ReportAllocs()

	for _, itemSize := range items {
		b.Run(fmt.Sprintf("items-%d", itemSize), func(b *testing.B) {
			for _, multiplier := range cacheSizeMultiplier {
				b.Run(fmt.Sprintf("multiplier-%fx", multiplier), func(b *testing.B) {
					for _, curr := range concurrencies {
						b.Run(fmt.Sprintf("curr-%d", curr), func(b *testing.B) {
							for _, alpha := range zipfAlphas {
								b.Run(fmt.Sprintf("alpha-%f", alpha), func(b *testing.B) {
									cacheSize := int(float64(itemSize) * multiplier)
									c := otto.New(32, cacheSize)

									var allHits, allMisses int64

									b.ResetTimer()
									for i := 0; i < b.N; i++ {
										hits, misses := run(c, itemSize, alpha, curr)
										allHits += hits
										allMisses += misses
									}

									b.ReportMetric(float64(allHits)/float64(allHits+allMisses)*100., "hit%")
								})
							}
						})
					}
				})
			}
		})
	}
}

func run(c otto.Cache, itemSize int, zipfAlpha float64, concurrency int) (int64, int64) {
	gen := NewZipfStringGenerator(uint64(itemSize), zipfAlpha)

	total := itemSize * workloadMultiplier
	each := total / concurrency

	// create keys in advance to not taint the QPS
	keys := make([][]string, concurrency)
	for i := 0; i < concurrency; i++ {
		keys[i] = make([]string, 0, each)
		for j := 0; j < each; j++ {
			keys[i] = append(keys[i], gen.Next())
		}
	}

	var wg sync.WaitGroup
	hits := make([]int64, concurrency)
	misses := make([]int64, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(k int) {
			for j := 0; j < each; j++ {
				key := keys[k][j]
				val := c.Get(key, nil)
				if val != nil {
					hits[k]++
				} else {
					misses[k]++
					c.Set(key, []byte{0xAA})
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	var totalHits, totalMisses int64
	for i := 0; i < concurrency; i++ {
		totalHits += hits[i]
		totalMisses += misses[i]
	}

	return totalHits, totalMisses
}

type ZipfStringGenerator struct {
	gen *ZipfGenerator
}

func NewZipfStringGenerator(size uint64, theta float64) *ZipfStringGenerator {
	src := rand.NewPCG(0, 1)
	r := rand.New(src)
	gen, err := NewZipfGenerator(r, 0, size, theta, false)

	if err != nil {
		panic(fmt.Errorf("could not create zipf generator: %v", err))
	}

	return &ZipfStringGenerator{
		gen: gen,
	}
}

func (z *ZipfStringGenerator) Next() string {
	return fmt.Sprintf("%d", z.gen.Uint64())
}
