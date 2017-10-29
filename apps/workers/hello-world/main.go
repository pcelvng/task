package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/launcher"
	"github.com/pcelvng/task/receivers/flex"
	"github.com/pcelvng/task/util"
)

func main() {

	// make a basic 'stdio' receiver (for reading and responding to the task bus)
	rcvrConfig := util.NewBusesConfig("stdio")
	rcvr, err := flex.NewFlexReceiver(rcvrConfig, "", "", "")
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// make the launcher
	lnchr, err := launcher.New(rcvr, launchFunc, nil)

	// signal handling, start and wait
	closeChan := make(chan os.Signal)
	signal.Notify(closeChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// start the launcher
	closedChan, err := lnchr.Start()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// start the task processing loop
	lnchr.Do()

	select {
	case <-closeChan:
		log.Println("shutting down...")
		lnchr.Close()
	case <-closedChan: // lnchr.Close called internally by the Launcher
		break
	}
	log.Println("done")
}

func launchFunc(tsk *task.Task) task.Worker {
	return &HelloWorldWorker{
		tsk:      tsk,
		taskChan: make(chan *task.Task),
	}
}

type HelloWorldWorker struct {
	tsk      *task.Task
	taskChan chan *task.Task
}

func (w *HelloWorldWorker) DoTask() chan *task.Task {
	go w.doTask()
	return w.taskChan
}

func (w *HelloWorldWorker) doTask() {
	w.tsk.Start()
	log.Printf("Hello, world! The time is '%v'", time.Now().String())
	w.tsk.Complete("I did it!")

	// Send the completed task back to the task bus.
	w.taskChan <- w.tsk
}

func (w *HelloWorldWorker) Close() error {
	return nil
}
