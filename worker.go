package task

import (
	"context"
	"fmt"
)

type Worker interface {
	// DoTask will return a Result and msg string.
	DoTask(context.Context) (Result, string)
}

// MakeWorker is called by the Launcher to
// generate a new worker for a new task.
type MakeWorker func(info string) Worker

// IsDone is a helper function that determines if ctx has been canceled
func IsDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// Interrupted is a helper function that can be called when DoTask was interrupted
func Interrupted() (Result, string) {
	return ErrResult, "task interrupted"
}

// Completed is a helper function that can be called when DoTask has completed
func Completed(format string, a ...interface{}) (Result, string) {
	return CompleteResult, fmt.Sprintf(format, a)
}
