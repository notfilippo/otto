# Otto

An S3-FIFO-based cache for big values in Go.

```go
package main

import (
	"fmt"

	"github.com/notfilippo/otto"
)

func main() {
    size := 1234
    cache := otto.New(size)

    cache.Set("key", []byte("value"))

    value := cache.Get("key", nil)
    fmt.Println(string(value))
}
```
