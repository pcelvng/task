package task

// Launcher is in charge of receiving
// tasks and launching workers to complete
// those tasks.
//
// Launcher should make sure that tasks
// are only requested from the receiver
// when a worker can start working on the
// immediately.
//
// Launcher is also responsible for shutting
// down the receiver and workers in such a way
// that the workers can safely complete and/or
// cleanup the task and respond with a task result.
type Launcher interface {
	// Do implements the launch routine.
	Do()

	// Start necessary connections.
	// the returned channel will be closed if the
	// launcher decides to shutdown.
	Start() (chan interface{}, error)

	// Close connections and safely shutdown workers
	// giving the workers time to cleanup in-progress
	// tasks and report on the result of the task.
	Close() error
}
