package task

import "context"

type Worker interface {
	// DoTask will return a Result and msg string.
	DoTask(context.Context) (Result, string)
}

// MakeWorker is called by the Launcher to
// generate a new worker for a new task.
type MakeWorker func(info string) Worker
