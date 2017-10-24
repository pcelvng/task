package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"github.com/pcelvng/task/launcher"
)

var (
	taskBus     = flag.String("task-bus", "stdio", "one of 'stdio', 'file', 'nsq'")
	taskInBus   = flag.String("in-bus", "", "one of 'stdin', 'file', 'nsq'; useful if you want the in and out bus to be different types.")
	taskOutBus  = flag.String("out-bus", "", "one of 'stdout', 'file', 'nsq'; useful if you want the in and out bus to be different types.")
	inBusFile   = flag.String("in-bus-file", "./in_tasks.json", "file bus path and name when 'file' task-bus specified")
	outBusFile  = flag.String("out-bus-file", "./out_tasks.json", "file bus path and name when 'file' task-bus specified")
	nsqdHosts   = flag.String("nsqd-hosts", "localhost:4150", "comma-separated list of nsqd hosts with port")
	taskType    = flag.String("task-type", "", "req'd: the task type the target worker knows how to accept")
	topic       = flag.String("topic", "", "default: 'task-type' value. topic to send the task (if applicable)")
	doneTopic   = flag.String("done-topic", "done", "default: 'done' value. topic to return the task after completion")
	failRate    = flag.Int("fail-rate", "0", "choose 0-100; the rate at which tasks will be marked with an error; does not support fractions of a percentage.")
	dur         = flag.String("duration", "1s", "duration a task will take to complete; 1s = 1 second, 1m = 1 minute, 1h = 1 hour")
	durVariance = flag.String("variance", "0s", "+/- evenly distributed variation when a task completes; 1s = 1 second, 1m = 1 minute, 1h = 1 hour")
)

func main() {
	flag.Parse()

	// load config
	c := NewConfig()
	c.BusType = *taskBus
	c.InBusType = *taskInBus
	c.OutBusType = *taskOutBus
	c.ReadPath = *inBusFile
	c.WritePath = *outBusFile
	c.TaskType = *taskType
	c.Topic = *topic
	c.DoneTopic = *doneTopic
	c.OutBusType = *taskBus
	c.FailRate = *failRate
	c.NsqdHostsString(*nsqdHosts)
	c.DurString(*dur)
	c.DurVarianceString(*durVariance)

	// create LauncherFunc
	lFn := NewLauncherFunc(*c)

	// create receiver


	// create launcher
	lConfig := launcher.NewConfig()
	lConfig.LaunchFunc = lFn



	closeChan := make(chan os.Signal)
	signal.Notify(closeChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)



	select {
	case <-closeChan:
		if err := bl.Close(); err != nil {
			log.Printf("err closing: '%v'\n", err.Error())
			os.Exit(1)
		}
	}
}
