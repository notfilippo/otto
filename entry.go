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

import "sync/atomic"

type entry struct {
	hash uint64
	size int
	slot int

	freq   atomic.Int32
	access atomic.Int32
}

// headerSize represents the size reserved at the start for each slot. That memory
// area stores a native-endian uint64 representing the next block linked to the
// current.
const headerSize = 8 // a.k.a. sizeof(uint64)

// newEntry creates a new entry as an any object. It is used in the
// cache's sync.Pool to instantiate new entries.
func newEntry() any {
	return new(entry)
}

// cost returns the slots needed to fit an entry of `entrySize` in
// a cache that has slots of `slotSize`. It takes into account the
// header size, which is stored alongside the cached values.
func cost(slotSize, entrySize int) int {
	return (entrySize + slotSize - headerSize - 1) / (slotSize - headerSize)
}
