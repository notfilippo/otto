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
	"hash/maphash"
	"math"
	"sync"
)

var (
	stripedBuffersSize = 4 * nextPowOf2(parallelism())
	stripedBuffersMask = stripedBuffersSize - 1
)

// ghost is a striped age-partitioned bloom filter.
type ghost struct {
	stripedBuffers []*stripedHashBuffer
	filter         *apbf
	mu             sync.RWMutex

	cap int
}

func newGhost(cap int) *ghost {
	w := float64(cap)
	l := float64(8)
	e := 0.01 // 1% false positive rate
	k := math.Ceil(-math.Log2(e))
	m := math.Ceil((w / l) * k / math.Ln2)

	buffers := make([]*stripedHashBuffer, 0, stripedBuffersSize)
	for range stripedBuffersSize {
		buffers = append(buffers, newStripedHashBuffer())
	}

	return &ghost{
		stripedBuffers: buffers,
		filter:         newApbf(int(k), int(l), int(m)),
		cap:            cap,
	}
}

func stripedIdx() int {
	return int(fastrand() & stripedBuffersMask)
}

func (g *ghost) Add(hash uint64) {
	buffer := g.stripedBuffers[stripedIdx()]
	result := buffer.Add(hash)
	if result != nil {
		g.mu.Lock()
		for _, hash := range result.Buffer {
			g.filter.Add(hash)
		}
		g.mu.Unlock()
		buffer.Free()
	}
}

func (g *ghost) In(hash uint64) bool {
	if !g.mu.TryRLock() {
		return false
	}

	ok := g.filter.Query(hash)
	g.mu.RUnlock()
	return ok
}

func (g *ghost) Clear() {
	g.filter.Clear()
}

type apbf struct {
	k, l, m int
	bits    []byte

	g, n, p int

	// Kirsch-Mitzenmacher Optimization.
	s1 maphash.Seed
	s2 maphash.Seed
}

func newApbf(k, l, m int) *apbf {
	bitsCount := (k + l) * m
	bits := make([]byte, (bitsCount+7)/8)
	g := int(float64(m) * math.Ln2 / float64(k))
	return &apbf{
		k:    k,
		l:    l,
		m:    m,
		bits: bits,
		g:    g,
		s1:   maphash.MakeSeed(),
		s2:   maphash.MakeSeed(),
	}
}

func (bf *apbf) shift() {
	slices := bf.k + bf.l

	if bf.p == 0 {
		bf.p = slices - 1
	} else {
		bf.p = bf.p - 1
	}

	bf.n = 0

	bit := bf.p * bf.m
	end := bit + bf.m

	for bit < end && bit%8 != 0 {
		bf.bclear(bit)
		bit += 1
	}

	for bit < end && bit+8 < end {
		bf.bits[bit/8] = 0
		bit += 8
	}

	for bit < end {
		bf.bclear(bit)
		bit += 1
	}
}

func (bf *apbf) Add(v uint64) {
	if bf.n >= bf.g {
		bf.shift()
	}

	slices := bf.k + bf.l
	x1, x2 := bf.hashes(v)

	for i := range bf.k {
		pos := (bf.p + i) % slices
		bit := pos*bf.m + bf.hash(x1, x2, i)
		bf.bset(bit)
	}

	bf.n += 1
}

func (bf *apbf) Query(v uint64) bool {
	slices := bf.k + bf.l

	i := bf.l
	prevCount := 0
	count := 0

	x1, x2 := bf.hashes(v)

	for {
		pos := bf.p + i
		if pos >= slices {
			pos -= slices
		}

		bit := pos*bf.m + bf.hash(x1, x2, i)

		if bf.bget(bit) {
			count += 1
			i += 1
			if prevCount+count == bf.k {
				return true
			}
		} else {
			if i < bf.k {
				return false
			}

			i -= bf.k
			prevCount = count
			count = 0
		}
	}
}

func (bf *apbf) Clear() {
	bf.n = 0
	bf.p = 0
	for i := range bf.bits {
		bf.bits[i] = 0
	}
}

func (bf *apbf) bset(i int) {
	bf.bits[i/8] |= 1 << (i % 8)
}

func (bf *apbf) bclear(i int) {
	bf.bits[i/8] &^= 1 << (i % 8)
}

func (bf *apbf) bget(i int) bool {
	return bf.bits[i/8]&(1<<(i%8)) != 0
}

func (bf *apbf) hashes(v uint64) (x1, x2 uint64) {
	x1 = maphash.Comparable(bf.s1, v)
	x2 = maphash.Comparable(bf.s2, v)
	return
}

func (bf *apbf) hash(x1, x2 uint64, i int) int {
	return int((x1 + uint64(i)*x2) % uint64(bf.m))
}
