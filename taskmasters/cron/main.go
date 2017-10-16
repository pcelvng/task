package main

import (
	"flag"
	"fmt"
	"os"

	"os/signal"
	"syscall"

	"github.com/robfig/cron"
	"github.com/pcelvng/task"
	"time"
)

var config = flag.String("config", "", "relative or absolute file path")

func main() {
	flag.Parse()
	if *config == "" {
		fmt.Println("'config' flag required")
		os.Exit(1)
	}

	conf, err := LoadConfig(*config)
	if err != nil {
		fmt.Printf("err parsing config: '%v'", err.Error())
		os.Exit(1)
	}

	c := cron.New()

	for rule := range conf.Rules {
		if err := c.AddJob("0 * * * * *", &JTask{}); err != nil {
			fmt.Printf("err parsing config: '%v'", err.Error())
			os.Exit(1)
		}
	}

	c.Start()

	closeChan := make(chan os.Signal)
	signal.Notify(closeChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	var sig os.Signal
	select {
	case sig = <-closeChan:
		c.Stop()
		fmt.Println("closing application")
		os.Exit(0)
	}
}

func NewJTask(r *Rule) (*JTask, error) {
	return &JTask{
		Rule: r,
	}, nil
}

type JTask struct {
	*Rule
}

func (j *JTask) Run() {
	// create Task
	tsk := task.New(j.TaskType, j.fmtTask())
}

func (j *JTask) fmtTask() string {
	now := time.Now()


}
