package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	start        = flag.String("start", "", "format: 'yyyy-mm-ddThh:00' (example: '2017-01-03T01:00')")
	end          = flag.String("end", "", "same format as start and if not specified will use start and end as the same value")
	taskBus      = flag.String("task-bus", "stdout", "one of 'stdout', 'file', 'nsq'")
	filePath     = flag.String("file-path", "./tasks.json", "file bus path and name when 'file' task-bus specified")
	nsqdHosts    = flag.String("nsqd-hosts", "localhost:4150", "comma-separated list of nsqd hosts with port")
	taskType     = flag.String("task-type", "", "req'd: the task type the target worker knows how to accept")
	taskTemplate = flag.String("task-template", "{yyyy}-{mm}-{dd}T{hh}:00", "task template")
	skipXHours   = flag.Uint("skip-x-hours", 0, "will generate tasks skipping x hours")
	onHours      = flag.String("on-hours", "", "comma separated list of hours to indicate which hours of a day to backload during a 24 period (each value must be between 0-23). Example '0,4,15' - will only generate tasks on hours 0, 4 and 15")
	topic        = flag.String("topic", "", "default: 'task-type' value. topic to send the task (if applicable)")
)

func main() {
	flag.Parse()

	// load config
	c := NewConfig()
	c.TaskType = *taskType
	c.TaskTemplate = *taskTemplate
	c.SkipXHours = int(*skipXHours)
	c.Topic = *topic
	c.BusType = *taskBus
	c.FilePath = *filePath
	c.NsqdHostsString(*nsqdHosts)
	if err := c.OnHoursString(*onHours); err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	if err := c.StartEndStrings(*start, *end); err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// create backloader
	bl, err := NewBackloader(c)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	closeChan := make(chan os.Signal)
	signal.Notify(closeChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		cnt, err := bl.Backload()
		log.Printf("loaded %v tasks\n", cnt)
		if err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}()

	select {
	case <-closeChan:
		// close the backloader
		if err := bl.Close(); err != nil {
			log.Printf("err closing: '%v'\n", err.Error())
			os.Exit(1)
		}
	}
}
