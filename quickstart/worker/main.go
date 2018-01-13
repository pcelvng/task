package main

import (
	"context"
	"log"

	"github.com/pcelvng/task"
)

func main() {
	l, _ := task.NewLauncher(MakeWorker, nil, nil)
	ctx, _ := l.DoTasks()
	<-ctx.Done()
}

func MakeWorker(info string) task.Worker {
	return &HelloWorldWorker{info}
}

type HelloWorldWorker struct {
	info string
}

func (w *HelloWorldWorker) DoTask(_ context.Context) (task.Result, string) {
	log.Println(w.info)
	return task.CompleteResult, "task complete!"
}
