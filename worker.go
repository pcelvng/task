package task

import "context"

type Worker interface {
	// DoTask will return a Result and msg string.
	DoTask() (Result, string)
}

// LaunchFunc is called by the Launcher to
// generate a new worker for a new task.
type LaunchFunc func(info string, wCtx context.Context) Worker
