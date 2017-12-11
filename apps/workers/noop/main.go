package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pcelvng/task"
)

func main() {
	// signal handling - capture signal early.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// config
	config = LoadConfig()
	if err := config.Validate(); err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// launcher
	l, err := task.NewLauncherWBus(MakeWorker, config.LauncherBusConfig)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	done, cncl := l.DoTasks()

	select {
	case <-sigChan:
		cncl()
		<-done.Done()
	case <-done.Done():
	}

	if err := l.Err(); err != nil {
		log.Printf("err in shutdown: '%v'\n", err.Error())
		os.Exit(1)
	}
	log.Println("done")
}
