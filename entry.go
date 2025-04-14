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
	"sync/atomic"
	"unsafe"
)

type entry struct {
	frequency atomic.Int32
	//lint:ignore U1000 prevents false sharing
	fpad [cacheLineSize - unsafe.Sizeof(atomic.Int32{})]byte

	access atomic.Int32
	//lint:ignore U1000 prevents false sharing
	apad [cacheLineSize - unsafe.Sizeof(atomic.Int32{})]byte

	next atomic.Int64
	// next should be padded enough by hash & size

	hash uint64
	size int
}

const (
	entryHeaderSize = int(unsafe.Sizeof(entry{}))
)

func cost(slotSize, entrySize int) int {
	return (entrySize + slotSize - 1) / slotSize
}
