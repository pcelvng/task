package main

import (
	"context"
	"log"

	"github.com/pcelvng/task"
)

func main() {
	l, _ := task.NewLauncher(newWorker, nil, nil)
	ctx, _ := l.DoTasks()
	<-ctx.Done()
}

func newWorker(info string) task.Worker {
	return &helloWorldWorker{info}
}

type helloWorldWorker struct {
	info string
}

func (w *helloWorldWorker) DoTask(_ context.Context) (task.Result, string) {
	log.Println(w.info)
	return task.CompleteResult, "task complete!"
}
