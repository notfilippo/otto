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

type taskKind uint8

const (
	addTask taskKind = iota + 1
	clearTask
	closeTask
)

// task is a set of information to update the cache:
// node, reason for write, difference after node cost change, etc.
type task struct {
	current *entry
	kind    taskKind
}

// newAddTask creates a task to add a node to policies.
func newAddTask(n *entry) task {
	return task{
		current: n,
		kind:    addTask,
	}
}

// newClearTask creates a task to clear policies.
func newClearTask() task {
	return task{
		kind: clearTask,
	}
}

// newCloseTask creates a task to clear policies and stop all goroutines.
func newCloseTask() task {
	return task{
		kind: closeTask,
	}
}
