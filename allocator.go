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
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

type allocator struct {
	arena []byte

	slotSize  int
	slotCount int

	fifo *queue[unsafe.Pointer]
}

func newAllocator(slotSize, slotCount int) *allocator {
	arenaSize := slotSize * slotCount
	arena, err := unix.Mmap(-1, 0, arenaSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		panic(fmt.Errorf("cannot allocate %d (slotSize) * %d (slotCount) = %d bytes via mmap: %s", slotSize, slotCount, arenaSize, err))
	}

	a := &allocator{
		arena:     arena,
		slotSize:  slotSize,
		slotCount: slotCount,
		fifo:      newQueue[unsafe.Pointer](slotCount),
	}

	a.Clear()

	return a
}

func (a *allocator) Alloc(slots int, yield func(unsafe.Pointer)) bool {
	return a.fifo.TryDequeueBatch(slots, yield)
}

func (a *allocator) Free(slot unsafe.Pointer) bool {
	return a.fifo.TryEnqueue(slot)
}

func (a *allocator) Clear() {
	for {
		if _, ok := a.fifo.TryDequeue(); !ok {
			break
		}
	}

	for i := range a.slotCount {
		if !a.fifo.TryEnqueue(unsafe.Pointer(&a.arena[i*a.slotSize : (i+1)*a.slotSize][0])) {
			panic("otto: allocator: failed to clear")
		}
	}
}

func (a *allocator) Close() error {
	return unix.Munmap(a.arena)
}
