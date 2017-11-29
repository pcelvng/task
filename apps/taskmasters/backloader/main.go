package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	c, err := LoadConfig()
	if err != nil {
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
		if err := bl.Stop(); err != nil {
			log.Printf("err closing: '%v'\n", err.Error())
			os.Exit(1)
		}
	}
}
