package main

import (
	"flag"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

var (
	tskType = flag.String("type", "", "task type")
	tskInfo = flag.String("info", "", "task info")
)

func main() {
	flag.Parse()

	tskJson, _ := task.New(*tskType, *tskInfo).Bytes()
	tskBus, _ := bus.NewBus(bus.NewBusConfig(""))
	tskBus.Send("", tskJson)
}
