package otto

import (
	"encoding/gob"
	"fmt"
	"hash/maphash"
	"io"
	"os"
	"unsafe"
)

type serializedEntry struct {
	Hash uint64
	Dead bool
	Buf  []byte
}

func SaveToFile(c Cache, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	if err := c.Serialize(file); err != nil {
		return fmt.Errorf("failed to serialize to file: %w", err)
	}

	return file.Close()
}

func LoadFromFile(path string) (Cache, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	cache, err := Deserialize(file)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize to file: %w", err)
	}

	return cache, file.Close()
}

func (c *cache) Serialize(w io.Writer) error {
	c.evictionLock.Lock()
	defer c.evictionLock.Unlock()

	e := gob.NewEncoder(w)

	if err := e.Encode(c.cap); err != nil {
		return err
	}

	if err := e.Encode(c.size); err != nil {
		return err
	}

	if err := e.Encode(c.windowSize); err != nil {
		return err
	}

	if err := e.Encode(c.protectedSize); err != nil {
		return err
	}

	if err := e.Encode(c.protectedCap); err != nil {
		return err
	}

	seed := *(*uint64)(unsafe.Pointer(&c.seed))
	if err := e.Encode(seed); err != nil {
		return err
	}

	entryDeques := []entryDeque{c.window, c.probation, c.protected}
	for _, deque := range entryDeques {
		var elements []serializedEntry
		for node := deque.head; node != nil; node = node.next {
			fmt.Println(node)
			elements = append(elements, serializedEntry{
				Hash: node.hash,
				Dead: node.dead.Load(),
				Buf:  node.buf.Bytes(),
			})
		}

		if err := e.Encode(elements); err != nil {
			return err
		}
	}

	return nil
}

func Deserialize(r io.Reader) (Cache, error) {
	d := gob.NewDecoder(r)

	var cap int
	if err := d.Decode(&cap); err != nil {
		return nil, err
	}

	var size int
	if err := d.Decode(&size); err != nil {
		return nil, err
	}

	var windowSize int
	if err := d.Decode(&windowSize); err != nil {
		return nil, err
	}

	var protectedSize int
	if err := d.Decode(&protectedSize); err != nil {
		return nil, err
	}

	var protectedCap int
	if err := d.Decode(&protectedCap); err != nil {
		return nil, err
	}

	var rawSeed uint64
	if err := d.Decode(&rawSeed); err != nil {
		return nil, err
	}

	seed := *(*maphash.Seed)(unsafe.Pointer(&rawSeed))

	c := New(cap).(*cache)
	c.seed = seed
	c.size = size
	c.windowSize = windowSize
	c.protectedSize = protectedSize
	c.protectedCap = protectedCap

	entryDeques := []*entryDeque{&c.window, &c.probation, &c.protected}
	dequeType := []queueType{queueWindow, queueProbation, queueProtected}
	for i, deque := range entryDeques {
		var elements []serializedEntry
		if err := d.Decode(&elements); err != nil {
			return nil, err
		}

		for _, element := range elements {
			entry := c.pool.Get().(*entry)
			entry.hash = element.Hash
			entry.dead.Store(element.Dead)
			entry.buf.Write(element.Buf)
			entry.queue = dequeType[i]
			deque.OfferBack(entry)
			c.data.Store(element.Hash, entry)
		}

	}

	return c, nil
}
