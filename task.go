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
