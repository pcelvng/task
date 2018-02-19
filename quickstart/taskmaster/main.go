package main

import (
	"flag"

	"github.com/pcelvng/task"
)

var (
	tskType = flag.String("type", "", "task type")
	tskInfo = flag.String("info", "", "task info")
)

func main() {
	flag.Parse()

	tskJson := task.New(*tskType, *tskInfo).JSONBytes()
	tskBus, _ := task.NewBus(task.NewBusOptions(""))
	tskBus.Send("", tskJson)
}
