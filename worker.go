package task

type Worker interface {
	// DoTask will actually carry out completing the
	// task. When the task is finished it is returned
	// on the return channel.
	//
	// Should not be called after Close
	// and must be called after Start.
	//
	// If the worker is not in a state to accept tasks
	// then it needs to send back the task with
	// an error result describing the problem.
	//
	// A worker needs to be safely willing to take on
	// more than one task.
	//
	// Each time DoTask is called a unique channel should
	// be returned specifically for listening on for that
	// completed task.
	DoTask(*Task) chan *Task

	// WorkerType provides the worker self
	// identifier that identifies what kind
	// of task it knows how to complete.
	WorkerType() string

	// Start will start up the worker by
	// making necessary connections etc
	// so that the worker is ready to accept
	// tasks.
	Start() error

	// Close will tell the worker to stop
	// working on the current task (if applicable),
	// do cleanup, and return the task uncompleted.
	//
	// If no task is in progress then
	Close() error
}

