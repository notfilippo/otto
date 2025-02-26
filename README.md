# Otto

An S3-FIFO-based cache for big values in Go.

```go
package main

import (
	"fmt"

	"github.com/notfilippo/otto"
)

func main() {
    chunkSize := 1024
    chunkCount := 256
    cache := otto.New(chunkSize, chunkCount)

    cache.Set("key", []byte("value"))

    value := cache.Get("key", nil)
    fmt.Println(string(value))
}
```
