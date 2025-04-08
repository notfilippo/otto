// Copyright (c) 2023 Alexey Mayshev. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otto

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	// The maximum number of elements per buffer.
	stripedCapacity = 16
	stripedMask     = uint64(stripedCapacity - 1)
)

// returnedSlice is the set of buffers returned by the lossy buffer.
type returnedSlice struct {
	Slice []*entry
}

// stripedBuffer is a circular ring buffer stores the elements being transferred by the producers to the consumer.
// The monotonically increasing count of reads and writes allow indexing sequentially to the next
// element location based upon a power-of-two sizing.
//
// The producers race to read the counts, check if there is available capacity, and if so then try
// once to CAS to the next write count. If the increment is successful then the producer lazily
// publishes the element. The producer does not retry or block when unsuccessful due to a failed
// CAS or the buffer being full.
//
// The consumer reads the counts and takes the available elements. The clearing of the elements
// and the next read count are lazily set.
//
// This implementation is striped to further increase concurrency.
type stripedBuffer struct {
	head atomic.Uint64
	//lint:ignore U1000 prevents false sharing
	headPadding [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte

	tail atomic.Uint64
	//lint:ignore U1000 prevents false sharing
	tailPadding [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte

	returned unsafe.Pointer
	//lint:ignore U1000 prevents false sharing
	returnedPadding [cacheLineSize - 8]byte

	returnedSlice unsafe.Pointer
	//lint:ignore U1000 prevents false sharing
	returnedSlicePadding [cacheLineSize - 8]byte

	buffer [stripedCapacity]unsafe.Pointer
}

func newStripedBuffer() *stripedBuffer {
	pb := &returnedSlice{
		Slice: make([]*entry, 0, stripedCapacity),
	}
	b := &stripedBuffer{
		returnedSlice: unsafe.Pointer(pb),
	}
	b.returned = b.returnedSlice
	return b
}

// Add lazily publishes the item to the consumer. Items may be lost due to contention.
func (b *stripedBuffer) Add(n *entry) []*entry {
	head := b.head.Load()
	tail := b.tail.Load()
	size := tail - head
	if size >= stripedCapacity {
		// full buffer
		return nil
	}
	if b.tail.CompareAndSwap(tail, tail+1) {
		// success
		//nolint:gosec // there will never be an overflow
		index := int(tail & stripedMask)
		atomic.StorePointer(&b.buffer[index], unsafe.Pointer(n))
		if size == stripedCapacity-1 {
			// try return new buffer
			if !atomic.CompareAndSwapPointer(&b.returned, b.returnedSlice, nil) {
				// somebody already get buffer
				return nil
			}

			returnedSlice := (*returnedSlice)(b.returnedSlice)
			for range stripedCapacity {
				//nolint:gosec // there will never be an overflow
				index := int(head & stripedMask)
				v := atomic.LoadPointer(&b.buffer[index])
				if v != nil {
					// published
					returnedSlice.Slice = append(returnedSlice.Slice, (*entry)(v))
					// release
					atomic.StorePointer(&b.buffer[index], nil)
				}
				head++
			}

			b.head.Store(head)
			return returnedSlice.Slice
		}
	}

	// failed
	return nil
}

// Free returns the processed buffer back and also clears it.
func (b *stripedBuffer) Free() {
	pb := (*returnedSlice)(b.returnedSlice)
	for i := range pb.Slice {
		pb.Slice[i] = nil
	}
	pb.Slice = pb.Slice[:0]
	atomic.StorePointer(&b.returned, b.returnedSlice)
}

// Clear clears the lossy Buffer and returns it to the default state.
func (b *stripedBuffer) Clear() {
	for !atomic.CompareAndSwapPointer(&b.returned, b.returnedSlice, nil) {
		runtime.Gosched()
	}
	for i := range stripedCapacity {
		atomic.StorePointer(&b.buffer[i], nil)
	}
	b.Free()
	b.tail.Store(0)
	b.head.Store(0)
}
