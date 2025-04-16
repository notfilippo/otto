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

type ghost struct {
	fifo    *queue[uint64]
	hashmap *hmap[struct{}]
	striped *stripedHashBuffer

	cap int
}

func newGhost(cap int) *ghost {
	return &ghost{
		fifo:    newQueue[uint64](cap),
		hashmap: newMap[struct{}](cap),
		striped: newStripedHashBuffer(),
		cap:     cap,
	}
}

func (g *ghost) Add(hash uint64) {
	result := g.striped.Add(hash)
	if result != nil {
		for _, hash := range result.Buffer {
			g.hashmap.Store(hash, struct{}{})

			for !g.fifo.TryEnqueue(hash) {
				e, ok := g.fifo.TryDequeue()
				if ok {
					g.hashmap.Delete(e)
				}
			}
		}

		g.striped.Free()
	}
}

func (g *ghost) In(hash uint64) bool {
	_, ok := g.hashmap.Load(hash)
	return ok
}

func (g *ghost) Clear() {
	g.hashmap.Clear()
	g.fifo = newQueue[uint64](g.cap)
}
