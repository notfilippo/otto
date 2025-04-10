package otto

import (
	"bytes"
	"testing"
)

func TestBasicBehaviour(t *testing.T) {
	c := New(100, 1<<12)
	defer c.Close()

	c.Set("key1", []byte("value1"))
	c.Set("key2", []byte("value2"))
	c.Set("key3", []byte("value3"))

	if value := c.Get("key1", nil); !bytes.Equal(value, []byte("value1")) {
		t.Fatalf("key1 should be in cache: %v", value)
	}

	if value := c.Get("key2", nil); !bytes.Equal(value, []byte("value2")) {
		t.Fatalf("key2 should be in cache: %v", value)
	}

	if value := c.Get("key3", nil); !bytes.Equal(value, []byte("value3")) {
		t.Fatalf("key3 should be in cache: %v", value)
	}
}

const HeaderSize = 16

func assertInCache(t *testing.T, c Cache, key string) {
	if c.Get(key, nil) == nil {
		t.Fatalf("key %s should be in cache", key)
	}
}

func assertNotInCache(t *testing.T, c Cache, key string) {
	if c.Get(key, nil) != nil {
		t.Fatalf("key %s should not be in cache", key)
	}
}

func TestCacheEvistion(t *testing.T) {
	size := 100
	c := New(1, 100)
	defer c.Close()

	// Lets' fill the cache.
	c.Set("key1", make([]byte, size-HeaderSize))

	assertInCache(t, c, "key1")

	// Now lets' add a new key to fill the cache again.
	c.Set("key2", make([]byte, size-HeaderSize))

	assertNotInCache(t, c, "key1")
	assertInCache(t, c, "key2")

	// Now lets' add a new key that does not completely fill the cache.
	c.Set("key3", make([]byte, 80-HeaderSize))

	assertNotInCache(t, c, "key2")
	assertInCache(t, c, "key3")

	// Now lets' add a new key that does fit in the space left by key3.
	c.Set("key4", make([]byte, 20-HeaderSize))

	assertInCache(t, c, "key3")
	assertInCache(t, c, "key4")

	// Now lets' add a new key that does not fit in the cache and evicts key3.
	c.Set("key5", make([]byte, 80-HeaderSize))

	assertNotInCache(t, c, "key3")
	assertInCache(t, c, "key4")
	assertInCache(t, c, "key5")

	// Now lets' add a new key that does not fit in the cache and evicts both key5 and key4.
	c.Set("key6", make([]byte, 80-HeaderSize))

	assertNotInCache(t, c, "key5")
	assertNotInCache(t, c, "key4")
	assertInCache(t, c, "key6")
}
