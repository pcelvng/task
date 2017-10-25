package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pcelvng/task/launcher"
	"github.com/pcelvng/task/receivers/flex"
)

var (
	taskBus     = flag.String("task-bus", "stdio", "one of 'stdio', 'file', 'nsq'")
	taskInBus   = flag.String("in-bus", "", "one of 'stdin', 'file', 'nsq'; useful if you want the in and out bus to be different types.")
	doneBus     = flag.String("done-bus", "", "one of 'stdout', 'file', 'nsq'; useful if you want the in and out bus to be different types.")
	inBusFile   = flag.String("in-bus-file", "./in_tasks.json", "file bus path and name when 'file' task-bus specified")
	outBusFile  = flag.String("out-bus-file", "./done.tasks.json", "file bus path and name when 'file' task-bus specified")
	nsqdHosts   = flag.String("nsqd-hosts", "localhost:4150", "comma-separated list of nsqd hosts with port")
	taskType    = flag.String("task-type", "", "req'd: the task type the target worker knows how to accept")
	taskTopic   = flag.String("task-topic", "", "default: 'task-type' value. topic to send the task (if applicable)")
	taskChannel = flag.String("task-channel", "", "default: 'task-type' value.")
	doneTopic   = flag.String("done-topic", "done", "topic to return the task after completion")
	failRate    = flag.Int("fail-rate", 0, "choose 0-100; the rate at which tasks will be marked with an error; does not support fractions of a percentage.")
	dur         = flag.String("duration", "1s", "duration a task will take to complete; 1s = 1 second, 1m = 1 minute, 1h = 1 hour")
	durVariance = flag.String("variance", "0s", "+ evenly distributed variation when a task completes; 1s = 1 second, 1m = 1 minute, 1h = 1 hour")
	workers     = flag.Int("workers", 1, "maximum number of workers running at one time; workers cannot be less than 1.")
)

func main() {
	flag.Parse()

	// load config
	c := NewConfig()
	c.BusType = *taskBus
	c.InBusType = *taskInBus
	c.OutBusType = *doneBus
	c.ReadPath = *inBusFile
	c.WritePath = *outBusFile
	c.TaskType = *taskType
	c.TaskTopic = *taskType // default value
	if *taskTopic != "" {
		c.TaskTopic = *taskTopic
	}
	c.TaskChannel = *taskType // default value
	if *taskChannel != "" {
		c.TaskChannel = *taskChannel
	}
	c.DoneTopic = *doneTopic
	c.FailRate = *failRate
	c.NsqdHostsString(*nsqdHosts)
	c.DurString(*dur)
	c.DurVarianceString(*durVariance)

	// create receiver
	rcvr, err := flex.NewFlexReceiver(c.BusesConfig, c.TaskTopic, c.TaskChannel, c.DoneTopic)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// create LauncherFunc
	lFn := NewLauncherFunc(*c)

	// create launcher
	lConfig := launcher.NewConfig()
	lConfig.LaunchFunc = lFn
	lConfig.Receiver = rcvr
	lConfig.MaxInFlight = *workers
	l, err := launcher.New(lConfig)
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
