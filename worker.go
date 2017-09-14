package task

// Launch will instantiate a worker and
// return it in a state ready to receive
// a task.
//
// Launch is called once per task. Every
// task gets its own worker. One worker per
// task and one task per worker.
type Launch func() (Worker, error)

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

	// Close will always be called; either while
	// a task is being worked on or (ie the app
	// needs to shut down all of a sudden)
	//
	// If Close is called and a task is being
	// worked on then the the worker needs to
	// cleanly stop working on the task. It also
	// needs to do what it can to make sure there
	// are little to no side-effects. It needs to
	// effectively revert state of the result space
	// to the state before the task was worked on.
	// This may not always be possible or it may not
	// even always matter. But a worker should always
	// be aware of side-effects and do what it can
	// to prevent them.
	//
	// Close is always called after a task is
	// completed.
	Close() error

	// WorkerType provides the worker self
	// identifier that identifies what kind
	// of task it knows how to complete.
	WorkerType() string
}
