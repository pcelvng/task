package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/robfig/cron"

	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/util"
)

var config = flag.String("config", "", "relative or absolute file path")

func main() {
	flag.Parse()
	if *config == "" {
		log.Println("'config' flag required")
		os.Exit(1)
	}

	conf, err := LoadConfig(*config)
	if err != nil {
		log.Printf("err parsing config: '%v'", err.Error())
		os.Exit(1)
	}

	// make producer
	p, err := util.NewProducer(conf.ProducersConfig)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// setup cron jobs
	c, err := MakeCron(conf.Rules, p)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// connect the producer
	if err := p.Connect(); err != nil {
		log.Printf("err on producer connect: '%v'\n", err.Error())
		os.Exit(1)
	}

	// start the cron
	c.Start()

	closeChan := make(chan os.Signal)
	signal.Notify(closeChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	select {
	case <-closeChan:
		log.Println("closing...")

		// stop the cron
		c.Stop()

		// close the producer
		if err := p.Close(); err != nil {
			log.Printf("err closing producer: '%v'\n", err.Error())
			os.Exit(1)
		}

		os.Exit(0)
	}
}

// MakeCron will create the cron and setup all the cron jobs.
// It will not start the cron.
func MakeCron(rules []*Rule, producer bus.Producer) (*cron.Cron, error) {
	c := cron.New()
	for _, rule := range rules {
		job := NewJob(rule, producer)
		if err := c.AddJob(rule.CronRule, job); err != nil {
			return nil, errors.New(fmt.Sprintf(
				"err parsing cron: '%v'",
				err.Error(),
			))
		}
	}

	return c, nil
}
