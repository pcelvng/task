package task

import (
	"context"
	"fmt"
)

// NewWorker is a worker initializer called by the Launcher to
// generate a new worker for a new task.
type NewWorker func(info string) Worker

type Worker interface {
	// DoTask will return a Result and msg string.
	DoTask(context.Context) (Result, string)
}

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
	return CompleteResult, fmt.Sprintf(format, a...)
}

func Failed(err error) (Result, string) {
	return ErrResult, err.Error()
}

// InvalidWorker is a helper function to indicate an error when calling MakerWorker
func InvalidWorker(format string, a ...interface{}) Worker {
	return &invalidWorker{
		message: fmt.Sprintf(format, a...),
	}
}

type invalidWorker struct {
	message string
}

func (w *invalidWorker) DoTask(_ context.Context) (Result, string) {
	return ErrResult, w.message
}

func IsInvalidWorker(w Worker) (bool, string) {
	i, ok := w.(*invalidWorker)
	if ok {
		return true, i.message
	}
	return false, ""
}
