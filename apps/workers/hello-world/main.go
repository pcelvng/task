package main

import (
	"context"
	"log"

	"github.com/pcelvng/task"
)

func main() {
	l, _ := task.NewLauncherWBus(
		launchFunc,
		task.NewLauncherBusConfig(""),
	)
	ctx, _ := l.DoTasks()
	<-ctx.Done()
}

func launchFunc(info string, _ context.Context) task.Worker {
	return &HelloWorldWorker{info}
}

type HelloWorldWorker struct {
	info string
}

func (w *HelloWorldWorker) DoTask() (task.Result, string) {
	log.Println(w.info)
	return task.CompleteResult, "task complete!"
}
