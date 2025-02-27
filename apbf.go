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
)

// apbf consists of a bit array partitioned into slices.
// Following three parameters determine the property of the structure:
//
// - `k`: number of slices filled for each insertion
// - `l`: number of slices besides the `k` slices above.
// - `m`: number of bits for each slice.
//
// Therefore the backing bit array is of size `(k + l) * m` bits.
type apbf struct {
	hasher kmhasher

	k uint64 // number of slices to fill for each insertion
	l uint64 // number of slices in addition to k slices
	m uint64 // number of bits for each slice

	n uint64 // counter
	p uint64 // position of the first logical slice on a bit vector
	g uint64 // generation

	bits []byte
}

func newApbf(k, l, m uint64) *apbf {
	if m%8 != 0 {
		panic("m must be a multiple of 8")
	}

	g := float64(m) * math.Ln2 / float64(k)
	bits := make([]byte, (k+l)*m)
	return &apbf{
		hasher: newKMHasher(),
		k:      k,
		l:      l,
		m:      m,
		n:      0,
		p:      0,
		g:      uint64(g),
		bits:   bits,
	}
}

func (a *apbf) shift() {
	nSlices := a.k + a.l
	var prev uint64
	if a.p == 0 {
		prev = nSlices - 1
	} else {
		prev = a.p - 1
	}

	slice := a.slice(prev)
	for i := range len(slice) {
		slice[i] = 0
	}

	if a.p == 0 {
		a.p = a.l + a.k - 1
	} else {
		a.p = a.p - 1
	}

	a.n = 0
}

func (a *apbf) slice(i uint64) []byte {
	p := i * a.m
	return a.bits[p : p+a.m]
}

func (a *apbf) insert(val string) {
	nSlices := a.k + a.l

	if a.n >= a.g {
		a.shift()
	}

	h := a.hasher.hash(val)
	for i := range a.k {
		pos := a.p + i
		if pos >= nSlices {
			pos -= nSlices
		}

		slice := a.slice(pos)
		hh := h.get(pos) % a.m
		slice[hh/8] |= 1 << (hh % 8)
	}

	a.n += 1
}

func (a *apbf) contains(val string) bool {
	nSlices := a.k + a.l
	i := a.l
	prevCount := uint64(0)
	count := uint64(0)

	h := a.hasher.hash(val)
	for {
		pos := a.p + i
		if pos >= nSlices {
			pos -= nSlices
		}

		slice := a.slice(pos)
		hh := h.get(pos) % a.m
		hit := (slice[hh/8] & (1 << (hh % 8))) != 0
		if hit {
			count += 1
			i += 1
			if prevCount + count == a.k {
				return true
			}
		} else {
			if i < a.k {
				return false
			}
			i -= a.k
			prevCount = count
			count = 0
		}
	}
}

func (a *apbf) window() uint64 {
	return a.l * a.g
}

func (a *apbf) slack() uint64 {
	return a.k * a.g
}

// Kirsch-Mitzenmacher optimization
type kmhasher struct {
	bh1, bh2 maphash.Seed
}

func newKMHasher() kmhasher {
	return kmhasher{
		bh1: maphash.MakeSeed(),
		bh2: maphash.MakeSeed(),
	}
}

type kmhashes struct {
	x1, x2 uint64
}

func (h kmhashes) get(i uint64) uint64 {
	return h.x1 + i*h.x2
}

func (h kmhasher) hash(val string) kmhashes {
	return kmhashes{
		x1: maphash.String(h.bh1, val),
		x2: maphash.String(h.bh2, val),
	}
}
