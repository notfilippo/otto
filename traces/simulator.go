package traces

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/notfilippo/otto"
)

// Run runs a trace for a cache. You can use a otto.TrackingCache to
// extract useful metrics to min-max.
func Run(cache otto.Cache, decoder *Decoder, workers int) {
	dispatch := make(chan Entry, 1<<10)

	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go worker(cache, dispatch)
	}

	for entry := range decoder.Iter {
		dispatch <- entry
	}

	wg.Wait()
}

func worker(cache otto.Cache, dispatch <-chan Entry) {
	var buf []byte
	for entry := range dispatch {
		switch entry.Op {
		case OpGet:
			res := cache.Get(fmt.Sprintf("%d", entry.Id), buf)
			runtime.KeepAlive(res)
		case OpSet:
			if cap(buf) < int(entry.Size) {
				buf = make([]byte, entry.Size)
			}
			cache.Set(fmt.Sprintf("%d", entry.Id), buf)
		default:
			panic(fmt.Sprintf("unexpected traces.Op: %#v", entry.Op))
		}
	}
}
