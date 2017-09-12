package task

// A Receiver is responsible for:
// - Connecting to the task bus
// - Mainting the task bus connection
// - Cleanly closing the connection
// - Reliably retrieving a single task message at a time
//   When Next is called in such a way that other workers do
//   not attempt to process the task
// - Reliably pushing out a task to the message bus
//
// It is not responsible for:
// - any kind of task validation
type Receiver interface {
	// Next Provides another Task if one is
	// available. Should be lazy loaded, where
	// applicable, so that other workers of the
	// same type in the ecosystem will be able to
	// work on an available task.
	//
	// If there is a problem retrieving the next Task
	// then Next() should return an error describing the problem.
	// If the receiver lost a connection to the task bus then
	// it should try to re-connect before returning an error.
	//
	// A receiver may communicate a message is the last one
	// by returning 'last' as true.
	Next() (tsk *Task, last bool, err error)

	// Done will accept a task and send it back to the
	// task bus.
	Done(*Task)

	// Connect connects to the task bus
	// but should not retrieve a task until
	// Next() is called.
	Connect() error

	// Close cleanly disconnects from the task bus.
	Close() error
}

// NoTasksError should be returned by a receiver
// when Next() is called and it is known that there are
// no more Tasks available.
type NoTasksError struct {}

func (e *NoTasksError) Error() string {
	return "task queue empty"
}
