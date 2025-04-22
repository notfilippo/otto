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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math/rand/v2"
	"sync/atomic"
	"testing"
)

const (
	testShardSize  = 512
	testShardCount = 512
	testEntrySize  = 64
)

func newCache(tb testing.TB, shardSize, shardCount uint32) Cache {
	cache := New(shardSize, shardCount)
	tb.Cleanup(func() {
		if err := cache.Close(); err != nil {
			tb.Fatalf("failed to close cache: %v", err)
		}
	})
	return cache
}

func defaultCache(tb testing.TB) Cache {
	return newCache(tb, testShardSize, testShardCount)
}

func key(i int) string {
	return fmt.Sprintf("key-%d", i)
}

var seed = maphash.MakeSeed()

func rng(key string) *rand.ChaCha8 {
	init := [32]byte{}
	binary.NativeEndian.PutUint64(init[:], maphash.String(seed, key))
	return rand.NewChaCha8(init)
}

func value(key string, size int) []byte {
	buf := make([]byte, size)
	rng(key).Read(buf)
	return buf
}

// cacheSet is a shortcut to set a key in the cache.
func cacheSet(_ testing.TB, cache Cache, key string, size int) {
	cache.Set(key, value(key, size))
}

// cacheHit is a shortcut to check if a key is in the cache.
func cacheHit(tb testing.TB, cache Cache, key string, size int) {
	v := cache.Get(key, nil)
	if v == nil {
		tb.Fatalf("expected %s to be in cache", key)
	}
	expected := value(key, size)
	if !bytes.Equal(v, expected) {
		tb.Fatalf("expected %s to have value %x instead found value %x", key, expected, v)
	}
}

// cacheMiss is a shortcut to check if a key is not in the cache.
func cacheMiss(tb testing.TB, cache Cache, key string) {
	v := cache.Get(key, nil)
	if v != nil {
		tb.Fatalf("expected %s to NOT be in cache (found: %s)", key, v)
	}
}

func TestSanity(t *testing.T) {
	c := defaultCache(t)
	cacheSet(t, c, "banana", testEntrySize)
	cacheHit(t, c, "banana", testEntrySize)
	cacheMiss(t, c, "apple")
}

func TestEviction(t *testing.T) {
	c := newCache(t, testShardSize, 1)

	// It should support a filled shard.
	cacheSet(t, c, "banana", testShardSize-headerSize)
	cacheHit(t, c, "banana", testShardSize-headerSize)

	// It should evict from a filled shard.
	cacheSet(t, c, "apple", 1)
	cacheHit(t, c, "apple", 1)
	cacheMiss(t, c, "banana")
}

func TestClear(t *testing.T) {
	c := defaultCache(t)
	cacheSet(t, c, "banana", testEntrySize)
	cacheHit(t, c, "banana", testEntrySize)
	c.Clear()
	cacheMiss(t, c, "banana")
}

func TestSerialize(t *testing.T) {
	c := defaultCache(t)
	cacheSet(t, c, "banana", testEntrySize)
	var buf bytes.Buffer
	err := c.Serialize(&buf)
	if err != nil {
		t.Fatalf("failed to serialize cache: %v", err)
	}

	c, err = Deserialize(&buf, testShardSize, testShardCount)
	if err != nil {
		t.Fatalf("failed to deserialize cache: %v", err)
	}
	cacheHit(t, c, "banana", testEntrySize)
}

func BenchmarkConcurrentGet(b *testing.B) {
	c := defaultCache(b)

	cacheSet(b, c, "banana", testEntrySize)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cacheHit(b, c, "banana", testEntrySize)
		}
	})
}

func BenchmarkConcurrentSet(b *testing.B) {
	c := defaultCache(b)
	var counter int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, testEntrySize)
			cacheSet(b, c, key(int(i)), testEntrySize)
		}
	})
}

func FuzzCache(f *testing.F) {
	// TODO(@notfilippo): maybe improve the corpus
	f.Add("key", []byte("value"))
	f.Fuzz(func(t *testing.T, key string, val []byte) {
		c := newCache(t, testShardSize, 1)
		c.Set(key, val)
		if len(val) <= testShardSize-headerSize {
			got := c.Get(key, nil)
			if !bytes.Equal(got, val) {
				t.Fatalf("unexpected result got `%s` (len %d), expected `%s` (len %d)", got, len(got), val, len(val))
			}
		}
	})
}
