package otto

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

type stripedHashResult struct {
	Buffer []uint64
}

type stripedHashBuffer struct {
	head atomic.Uint64
	_    [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte

	tail atomic.Uint64
	_    [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte

	returned unsafe.Pointer
	_        [cacheLineSize - unsafe.Sizeof(uintptr(0))]byte

	result unsafe.Pointer
	_      [cacheLineSize - unsafe.Sizeof(uintptr(0))]byte

	buffer []uint64
}

const (
	stripedCapacity = 16
	stripedMask     = stripedCapacity - 1
)

func newStripedHashBuffer() *stripedHashBuffer {
	result := &stripedHashResult{
		Buffer: make([]uint64, 0, stripedCapacity),
	}

	b := &stripedHashBuffer{
		result: unsafe.Pointer(result),
		buffer: make([]uint64, stripedCapacity),
	}

	b.returned = b.result
	return b
}

func (b *stripedHashBuffer) Add(n uint64) *stripedHashResult {
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
		atomic.StoreUint64(&b.buffer[index], n)
		if size == stripedCapacity-1 {
			// try return new buffer
			if !atomic.CompareAndSwapPointer(&b.returned, b.result, nil) {
				// somebody already get buffer
				return nil
			}

			pb := (*stripedHashResult)(b.result)
			for range stripedCapacity {
				//nolint:gosec // there will never be an overflow
				index := int(head & stripedMask)
				v := atomic.LoadUint64(&b.buffer[index])
				if v != 0 {
					// published
					pb.Buffer = append(pb.Buffer, v)
					// release
					atomic.StoreUint64(&b.buffer[index], 0)
				}
				head++
			}

			b.head.Store(head)
			return pb
		}
	}

	// failed
	return nil
}

func (b *stripedHashBuffer) Free() {
	pb := (*stripedHashResult)(b.result)
	for i := range pb.Buffer {
		pb.Buffer[i] = 0
	}

	pb.Buffer = pb.Buffer[:0]
	atomic.StorePointer(&b.returned, b.result)
}

func (b *stripedHashBuffer) Clear() {
	for !atomic.CompareAndSwapPointer(&b.returned, b.result, nil) {
		runtime.Gosched()
	}

	for i := range stripedCapacity {
		atomic.StoreUint64(&b.buffer[i], 0)
	}

	b.Free()
	b.tail.Store(0)
	b.head.Store(0)
}
