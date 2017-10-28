package task

// LaunchFunc is called by the Launcher to
// generate a fresh new worker specifically
// for that task.
//
// LaunchFunc is called once per task. Every
// task gets its own worker. One worker per
// task and one task per worker. Although,
// the worker implementation can get around this
// if it really needs/wants to.
//
// If there is an error launching the worker
// (like connection problems) then the worker
// should report the error via the task and
// still return the task through the channel
// returned from DoTask.
//
// *Task is required to create a worker to emphasize
// that there should generally be one task per
// worker and one worker per task.
type LaunchFunc func(*Task) Worker

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
	//
	// Any error that affects the success of completing a
	// task should be reported via the task by calling
	// Task.Err(msg). Once a worker is launched with a task
	// it commits to reporting on the outcome of that task
	// regardless of outcome.
	//
	// Common error scenarios:
	// - Failure launching
	// - Failure to complete task
	// - Task completed but outcome is not as expected
	// - Worker.Close() is called before the task is
	//   completed.
	DoTask() chan *Task

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
