package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var config = flag.String("config", "config.toml", "relative or absolute file path")

func main() {
	flag.Parse()
	if *config == "" {
		log.Println("'config' flag value required")
		os.Exit(1)
	}

	conf, err := LoadConfig(*config)
	if err != nil {
		log.Printf("err parsing config: '%v'", err.Error())
		os.Exit(1)
	}

	// make retryer
	rtryr, err := NewRetryer(conf)
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
