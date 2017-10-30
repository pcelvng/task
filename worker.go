package task

import "context"

type Worker interface {
	// DoTask will return a done task.
	DoTask() Task
}

// LaunchFunc is called by the Launcher to
// generate a fresh new worker for a new task.
//
// LaunchFunc is called once per task. Every
// task gets its own worker. One worker per
// task and one task per worker.
//
// If there is an error launching the worker
// (like connection problems) then the worker
// should report the error via the returned
// DoTask() task.
//
// The launcher will close the context after getting
// back the done task.
type LaunchFunc func(Task, context.Context) Worker
