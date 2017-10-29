package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pcelvng/task/launcher"
	"github.com/pcelvng/task/receivers/flex"
)

func main() {
	c := LoadConfig()
	if err := c.Validate(); err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// create receiver
	rcvr, err := flex.NewFlexReceiver(c.BusesConfig, c.Topic, c.Channel, c.DoneTopic)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// create LauncherFunc
	lFn := NewLauncherFunc(*c)

	// create launcher
	lConfig := launcher.NewConfig()
	lConfig.MaxInFlight = *workers
	l, err := launcher.New(rcvr, lFn, lConfig)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// signal handling, start and wait
	closeChan := make(chan os.Signal)
	signal.Notify(closeChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	closedChan, err := l.Start()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	l.Do()

	select {
	case <-closeChan:
		log.Println("shutting down...")
		l.Close()
	case <-closedChan: // l.Close called internally by the Launcher
		break
	}
	log.Println("done")
}
