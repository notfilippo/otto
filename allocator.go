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

	"golang.org/x/sys/unix"
)

type allocator struct {
	arena []byte
	fifo  *queue[*byte]

	chunkSize  int
	chunkCount int
}

func newAllocator(chunkSize, chunkCount int) *allocator {
	arenaSize := chunkSize * chunkCount
	arena, err := unix.Mmap(-1, 0, arenaSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		panic(fmt.Errorf("cannot allocate %d (chunkSize) * %d (chunkCount) = %d bytes via mmap: %s", chunkSize, chunkCount, arenaSize, err))
	}

	a := &allocator{
		arena:      arena,
		chunkSize:  chunkSize,
		chunkCount: chunkCount,
	}
	a.Clear()

	return a
}

func (a *allocator) Clear() {
	fifo := newQueue[*byte](a.chunkCount)
	for i := 0; i < a.chunkCount; i++ {
		fifo.MustPush(&a.arena[i*a.chunkSize : (i+1)*a.chunkSize][0], 1)
	}
	a.fifo = fifo
}

func (a *allocator) Get(chunks []*byte) []*byte {
	need := cap(chunks)
	chunks = chunks[:0]

	for i := 0; i < need; i++ {
		chunk, ok := a.fifo.TryPop()
		if !ok || chunk == nil {
			break
		}

		chunks = append(chunks, chunk)
	}

	if len(chunks) != need {
		for _, chunk := range chunks {
			a.fifo.MustPush(chunk, 1)
		}

		return chunks[:0]
	}

	return chunks
}

func (a *allocator) Put(chunk *byte) {
	a.fifo.MustPush(chunk, 1)
}
