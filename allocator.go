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
	"unsafe"
)

type allocator struct {
	arena     []byte
	slotSize  int
	slotCount int

	fifo *queue[*byte]
}

func newAllocator(slotSize, slotCount int) *allocator {
	arenaSize := slotSize * slotCount
	arena := make([]byte, arenaSize)

	a := &allocator{
		arena:     arena,
		slotSize:  slotSize,
		slotCount: slotCount,
		fifo:      newQueue[*byte](slotCount),
	}

	a.Clear()

	return a
}

func (a *allocator) Alloc(slots int, yield func(*byte)) bool {
	return a.fifo.TryDequeueBatch(slots, func(b *byte) {
		arenaPtr := uintptr(unsafe.Pointer(&a.arena[0]))
		slotPtr := uintptr(unsafe.Pointer(b))
		slotSize := uintptr(a.slotSize)
		offset := slotPtr - arenaPtr
		if slotPtr < arenaPtr || offset%slotSize != 0 {
			panic("otto: inviariant violated: unaligned alloc")
		}

		yield(b)
	})
}

func (a *allocator) Free(slot *byte) bool {
	arenaPtr := uintptr(unsafe.Pointer(&a.arena[0]))
	slotPtr := uintptr(unsafe.Pointer(slot))
	slotSize := uintptr(a.slotSize)
	offset := slotPtr - arenaPtr
	if slotPtr < arenaPtr || offset%slotSize != 0 {
		panic("otto: inviariant violated: unaligned free")
	}
	return a.fifo.TryEnqueue(slot)
}

func (a *allocator) Clear() {
	for {
		if _, ok := a.fifo.TryDequeue(); !ok {
			break
		}
	}

	for i := range a.slotCount {
		if !a.fifo.TryEnqueue(&a.arena[i*a.slotSize : (i+1)*a.slotSize][0]) {
			panic("otto: allocator: failed to clear")
		}
	}
}

func (a *allocator) Close() error {
	return nil
}
