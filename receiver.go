package task

import "context"

// Receiver is responsible for:
// - Connecting to the task bus
// - Maintaining the task bus connection
// - Cleanly closing the connection
// - Lazy loading tasks
// - Reliably pushing out a task to the task bus
// on the configured 'done' topic.
//
// It is not responsible for:
// - any kind of task validation
type Receiver interface {
	// Connect connects to the task bus
	// but should not retrieve a task until
	// Next() is called. The context is needed for
	// a clean shutdown.
	//
	// Connecting can also be done on initialization but
	// by providing a Connect method here there is an avenue
	// for providing context for a clean shutdown.
	Connect(context.Context) error

	// Next Provides another Task if one is
	// available. Should be lazy loaded so that
	// the task can be worked on by an available worker.
	//
	// Returning 'done' = true means there are no more tasks.
	//
	// A task may still be returned when 'done' = true.
	Next() (tsk *Task, done bool, err error)

	// Done pushes the task to the configured 'done' topic.
	//
	// The receiver will not check that the task has populated
	// done fields.
	Done(*Task)
}
