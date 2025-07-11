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
	"encoding/gob"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"os"
	"unsafe"
)

const serializeVersionHeader1_0_0 = "otto-cache-1.0.0"
const serializeVersionHeader = "otto-cache-1.0.1"

type serializedEntry struct {
	Key   uint64
	Value []byte
	Freq  int32
}

func (c *cache) SerializeV100(w io.Writer) error {
	e := gob.NewEncoder(w)

	if err := e.Encode(serializeVersionHeader1_0_0); err != nil {
		return fmt.Errorf("failed to encode version header: %w", err)
	}

	seed := *(*uint64)(unsafe.Pointer(&c.seed))
	if err := e.Encode(seed); err != nil {
		return fmt.Errorf("failed to encode seed: %w", err)
	}

	err := c.hashmap.Range(func(k uint64, v int32) error {
		se := serializedEntry{
			Key:   k,
			Value: c.read(v, nil),
			Freq:  c.eFreq[v].Load(),
		}

		return e.Encode(se)
	})

	if err != nil {
		return fmt.Errorf("failed to encode entry: %w", err)
	}

	return err
}

type serializedEntryV101 struct {
	Value []byte
	Freq  int32
	Hash  uint64
}

func (c *cache) Serialize(w io.Writer) error {
	e := gob.NewEncoder(w)

	if err := e.Encode(serializeVersionHeader); err != nil {
		return fmt.Errorf("failed to encode version header: %w", err)
	}

	seed := *(*uint64)(unsafe.Pointer(&c.seed))
	if err := e.Encode(seed); err != nil {
		return fmt.Errorf("failed to encode seed: %w", err)
	}

	if err := c.serializeQueue(e, c.m); err != nil {
		return err
	}

	if err := c.serializeQueue(e, c.s); err != nil {
		return err
	}

	if err := c.serializeGhostQueue(e); err != nil {
		return err
	}

	return nil
}

// Serializes the given queue from tail to head.
func (c *cache) serializeQueue(e *gob.Encoder, q *queue[int32]) error {
	if err := e.Encode(q.Len()); err != nil {
		return fmt.Errorf("failed to encode queue size: %w", err)
	}

	for entryIdx := range q.All() {
		entry := serializedEntryV101{
			Value: c.read(entryIdx, nil),
			Freq:  c.eFreq[entryIdx].Load(),
			Hash:  c.eHash[entryIdx],
		}

		if err := e.Encode(entry); err != nil {
			return fmt.Errorf("failed to encode queue entry: %w", err)
		}
	}

	return nil
}

func (c *cache) serializeGhostQueue(e *gob.Encoder) error {
	if err := e.Encode(c.g.cap); err != nil {
		return fmt.Errorf("failed to encode ghost queue capacity: %w", err)
	}

	// We only serialize the apbf structure, not the intermediate buffers
	if err := e.Encode(c.g.filter.k); err != nil {
		return fmt.Errorf("failed to encode ghost queue apbf: %w", err)
	}

	if err := e.Encode(c.g.filter.l); err != nil {
		return fmt.Errorf("failed to encode ghost queue apbf: %w", err)
	}

	if err := e.Encode(c.g.filter.m); err != nil {
		return fmt.Errorf("failed to encode ghost queue apbf: %w", err)
	}

	if err := e.Encode(c.g.filter.bits); err != nil {
		return fmt.Errorf("failed to encode ghost queue apbf: %w", err)
	}

	if err := e.Encode(c.g.filter.g); err != nil {
		return fmt.Errorf("failed to encode ghost queue apbf: %w", err)
	}

	if err := e.Encode(c.g.filter.n); err != nil {
		return fmt.Errorf("failed to encode ghost queue apbf: %w", err)
	}

	if err := e.Encode(c.g.filter.p); err != nil {
		return fmt.Errorf("failed to encode ghost queue apbf: %w", err)
	}

	if err := e.Encode(*(*uint64)(unsafe.Pointer(&c.g.filter.s1))); err != nil {
		return fmt.Errorf("failed to encode ghost queue apbf: %w", err)
	}

	if err := e.Encode(*(*uint64)(unsafe.Pointer(&c.g.filter.s2))); err != nil {
		return fmt.Errorf("failed to encode ghost queue apbf: %w", err)
	}

	return nil
}

// SaveToFile serializes the cache as a file in the provided path.
func SaveToFile(c Cache, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	if err := c.Serialize(file); err != nil {
		return fmt.Errorf("failed to serialize to file: %w", err)
	}

	return nil
}

// Deserialize deserializes the cache from a byte stream.
// Refer to the New method for the usage of slotSize & slotCount arguments.
func Deserialize(r io.Reader, slotSize, slotCount int32) (Cache, error) {
	mCapacity, sCapacity := defaultEx(slotCount)
	return DeserializeEx(r, slotSize, mCapacity, sCapacity)
}

// DeserializeEx deserializes the cache from a byte stream.
// Refer to the NewEx method for the usage of slotSize & mCap & sCap arguments.
func DeserializeEx(r io.Reader, slotSize, mCap, sCap int32) (Cache, error) {
	d := gob.NewDecoder(r)

	var versionHeader string
	if err := d.Decode(&versionHeader); err != nil {
		return nil, fmt.Errorf("failed to decode version header: %w", err)
	}

	switch versionHeader {
	case serializeVersionHeader1_0_0:
		return DeserializeExV100(d, slotSize, mCap, sCap)
	case serializeVersionHeader:
		return DeserializeExV101(d, slotSize, mCap, sCap)
	default:
		return nil, fmt.Errorf("unsupported version header: %s", versionHeader)
	}

}

// Deserializer for version otto-cache-1.0.0
func DeserializeExV100(d *gob.Decoder, slotSize, mCap, sCap int32) (Cache, error) {

	var rawSeed uint64
	if err := d.Decode(&rawSeed); err != nil {
		return nil, fmt.Errorf("failed to decode seed: %w", err)
	}

	seed := *(*maphash.Seed)(unsafe.Pointer(&rawSeed))

	c := NewEx(slotSize, mCap, sCap).(*cache)
	c.seed = seed

	for {
		var se serializedEntry
		if err := d.Decode(&se); err != nil {
			if err == io.EOF {
				break // End of stream
			}

			return nil, fmt.Errorf("failed to decode entry: %w", err)
		}

		clampedFreq := max(min(se.Freq, frequencyMax), frequencyMin)

		err := c.set(se.Key, se.Value, clampedFreq, false)
		if err != nil {
			return nil, fmt.Errorf("failed to add entry during decoding: %w", err)
		}
	}

	return c, nil
}

// Deserializer for version otto-cache-1.0.1
func DeserializeExV101(d *gob.Decoder, slotSize, mCap, sCap int32) (Cache, error) {

	var rawSeed uint64
	if err := d.Decode(&rawSeed); err != nil {
		return nil, fmt.Errorf("failed to decode seed: %w", err)
	}

	seed := *(*maphash.Seed)(unsafe.Pointer(&rawSeed))

	c := NewEx(slotSize, mCap, sCap).(*cache)
	c.seed = seed

	// Recover the elements from the m-queue
	var mLen int
	if err := d.Decode(&mLen); err != nil {
		return nil, fmt.Errorf("failed to decode m queue size: %w", err)
	}

	for i := 0; i < mLen; i++ {
		var entry serializedEntryV101
		if err := d.Decode(&entry); err != nil {
			return nil, fmt.Errorf("failed to decode m queue entry: %w", err)
		}

		err := c.set(entry.Hash, entry.Value, entry.Freq, true)
		if err != nil {
			return nil, fmt.Errorf("failed to add entry to m queue during decoding: %w", err)
		}
	}

	// Recover the elements from the s-queue
	var sLen int
	if err := d.Decode(&sLen); err != nil {
		return nil, fmt.Errorf("failed to decode s queue size: %w", err)
	}

	for i := 0; i < sLen; i++ {
		var entry serializedEntryV101
		if err := d.Decode(&entry); err != nil {
			return nil, fmt.Errorf("failed to decode m queue entry: %w", err)
		}

		err := c.set(entry.Hash, entry.Value, entry.Freq, false)
		if err != nil {
			return nil, fmt.Errorf("failed to add entry to m queue during decoding: %w", err)
		}
	}

	if err := c.deserializeGhostQueue(d); err != nil {
		return nil, fmt.Errorf("failed to decode ghost queue: %w", err)
	}

	return c, nil
}

func (c *cache) deserializeGhostQueue(d *gob.Decoder) error {
	var ghostCap int
	if err := d.Decode(&ghostCap); err != nil {
		return fmt.Errorf("failed to decode ghost queue capacity: %w", err)
	}

	if ghostCap != c.g.cap {
		// We don't want a ghost queue with a different capacity, so we just avoid deserializing it.
		return nil
	}

	// Decode apbf structure
	if err := d.Decode(&c.g.filter.k); err != nil {
		return fmt.Errorf("failed to decode ghost queue apbf: %w", err)
	}
	if err := d.Decode(&c.g.filter.l); err != nil {
		return fmt.Errorf("failed to decode ghost queue apbf: %w", err)
	}
	if err := d.Decode(&c.g.filter.m); err != nil {
		return fmt.Errorf("failed to decode ghost queue apbf: %w", err)
	}
	if err := d.Decode(&c.g.filter.bits); err != nil {
		return fmt.Errorf("failed to decode ghost queue apbf: %w", err)
	}
	if err := d.Decode(&c.g.filter.g); err != nil {
		return fmt.Errorf("failed to decode ghost queue apbf: %w", err)
	}
	if err := d.Decode(&c.g.filter.n); err != nil {
		return fmt.Errorf("failed to decode ghost queue apbf: %w", err)
	}
	if err := d.Decode(&c.g.filter.p); err != nil {
		return fmt.Errorf("failed to decode ghost queue apbf: %w", err)
	}

	var s1, s2 uint64
	if err := d.Decode(&s1); err != nil {
		return fmt.Errorf("failed to decode ghost queue apbf: %w", err)
	}
	if err := d.Decode(&s2); err != nil {
		return fmt.Errorf("failed to decode ghost queue apbf: %w", err)
	}
	c.g.filter.s1 = *(*maphash.Seed)(unsafe.Pointer(&s1))
	c.g.filter.s2 = *(*maphash.Seed)(unsafe.Pointer(&s2))

	return nil
}

// LoadFromFile deserializes the cache from the file at the provided path.
// Refer to the New method for the usage of slotSize & slotCount arguments.
func LoadFromFile(path string, slotSize, slotCount int32) (Cache, error) {
	mCap, sCap := defaultEx(slotCount)
	return LoadFromFileEx(path, slotSize, mCap, sCap)
}

// LoadFromFileEx deserializes the cache from the file at the provided path.
// Refer to the NewEx method for the usage of slotSize & mCap & sCap arguments.
func LoadFromFileEx(path string, slotSize, mCap, sCap int32) (Cache, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	cache, err := DeserializeEx(file, slotSize, mCap, sCap)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize from file: %w", err)
	}

	return cache, nil
}
