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

import "math"

type frequencySketch struct {
	table []uint64
	size  int
}

func newFrequencySketch(capacity int) *frequencySketch {
	capacityRounded := roundUpPowerOf2(uint32(capacity))
	return &frequencySketch{
		table: make([]uint64, capacityRounded),
		size:  0,
	}
}

func (f *frequencySketch) Frequency(key uint64) uint64 {
	// This can happen as frequency sketch is lazy-initialized.
	if f == nil {
		return 0
	}

	frequency := uint64(math.MaxUint64)
	for i := range 4 {
		frequency = min(frequency, f.getCount(key, i))
	}

	return frequency
}

func (f *frequencySketch) Increment(key uint64) {
	// This can happen as frequency sketch is lazy-initialized.
	if f == nil {
		return
	}

	wasIncremented := false
	for i := range 4 {
		wasIncremented = f.tryIncrement(key, i)
	}

	if wasIncremented {
		f.size += 1
		if f.size == len(f.table)*10 {
			f.reset()
		}
	}
}

func (f *frequencySketch) reset() {
	for i := range len(f.table) {
		f.table[i] = (f.table[i] >> 1) & 0x7777777777777777
	}

	f.size /= 2
}

func (f *frequencySketch) tryIncrement(key uint64, counterIndex int) bool {
	index := f.tableIndex(key, counterIndex)
	offset := f.counterOffset(key, counterIndex)
	if f.canIncrementCounterAt(index, offset) {
		f.table[index] += 1 << offset
		return true
	}

	return false
}

func (f *frequencySketch) canIncrementCounterAt(index int, offset uint64) bool {
	mask := uint64(0xF) << offset
	return (f.table[index] & mask) != mask
}

func (f *frequencySketch) getCount(key uint64, counterIndex int) uint64 {
	index := f.tableIndex(key, counterIndex)
	offset := f.counterOffset(key, counterIndex)
	return (f.table[index] >> offset) & 0xF
}

var seeds = []uint64{
	0xc3a5c85c97cb3127,
	0xb492b66fbe98f273,
	0x9ae16a3b2f90404f,
	0xcbf29ce484222325,
}

func (f *frequencySketch) tableIndex(key uint64, counterIndex int) int {
	h := seeds[counterIndex] * key
	h += h >> 32
	return int(h & uint64(len(f.table)-1))
}

func (f *frequencySketch) counterOffset(key uint64, counterIndex int) uint64 {
	return (offsetMultiplier(key) + uint64(counterIndex)) << 2
}

func (f *frequencySketch) SampleSize() int {
	return len(f.table) * 10
}

func offsetMultiplier(key uint64) uint64 {
	return (key & 3) << 3
}
