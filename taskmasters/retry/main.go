package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

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

	// make consumer
	c, err := util.NewConsumer(conf.BusesConfig)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// make producer
	p, err := util.NewProducer(conf.BusesConfig)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// make retryer
	rtryr := NewRetryer(c, p, conf.RetryRules)
	err = rtryr.Start()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	closeChan := make(chan os.Signal)
	signal.Notify(closeChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	select {
	case <-closeChan:
		log.Println("closing...")

		// close the retryer
		if err := rtryr.Close(); err != nil {
			log.Printf("err closing retryer: '%v'\n", err.Error())
			os.Exit(1)
		}

		os.Exit(0)
	}
}
