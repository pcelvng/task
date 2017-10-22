package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	iobus "github.com/pcelvng/task/bus/io"
	nsqbus "github.com/pcelvng/task/bus/nsq"
)

// NewBackloader will validate the config, create and connect the
// bus producer.
func NewBackloader(conf *Config) (*Backloader, error) {
	// validate config
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	// create producer and connect
	p, err := MakeProducer(conf)
	if err != nil {
		return nil, err
	}

	err = p.Connect()
	if err != nil {
		return nil, err
	}

	return &Backloader{
		busProducer: p,
		config:      conf,
	}, nil
}

type Backloader struct {
	config      *Config
	busProducer bus.Producer
}

// Backload returns 'int' which represents the number of
// tasks sent to the task bus. If start == end then one
// task will be sent.
func (bl *Backloader) Backload() (int, error) {
	// backload loop
	atHour := bl.config.Start
	endHour := bl.config.End
	cnt := 0
	for {
		// task value
		tskValue := bl.fmtTask(atHour)

		// create task
		tsk := task.New(bl.config.TaskType, tskValue)

		// send to bus
		topic := bl.config.TaskType
		if bl.config.Topic != "" {
			topic = bl.config.Topic
		}

		msg, err := tsk.Bytes()
		if err != nil {
			return cnt, err
		}

		if err := bl.busProducer.Send(topic, msg); err != nil {
			return cnt, err
		}
		cnt = cnt + 1

		// increment atHour
		atHour = atHour.Add(time.Hour)

		// check if the loop is finished
		diff := int(endHour.Sub(atHour))
		if diff < 0 {
			return cnt, nil
		}
	}
}

func (bl *Backloader) Close() error {
	if bl.busProducer != nil {
		err := bl.busProducer.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (bl *Backloader) fmtTask(dateHour time.Time) string {

	// substitute year {yyyy}
	y := strconv.Itoa(dateHour.Year())
	s := strings.Replace(bl.config.TaskTemplate, "{yyyy}", y, -1)

	// substitute year {yy}
	s = strings.Replace(s, "{yy}", y[2:], -1)

	// substitute month {mm}
	m := fmt.Sprintf("%02d", int(dateHour.Month()))
	s = strings.Replace(s, "{mm}", m, -1)

	// substitute day {dd}
	d := fmt.Sprintf("%02d", dateHour.Day())
	s = strings.Replace(s, "{dd}", d, -1)

	// substitute hour {hh}
	h := fmt.Sprintf("%02d", dateHour.Hour())
	s = strings.Replace(s, "{hh}", h, -1)

	return s
}

// MakeProducer will attempt to create the desired bus
// producer and connect the producer so that it is ready to
// use.
func MakeProducer(conf *Config) (bus.Producer, error) {
	// setup bus producer
	var p bus.Producer
	var err error
	switch conf.TaskBus {
	case "stdout", "":
		p = iobus.NewStdoutProducer()
		break
	case "file":
		p, err = iobus.NewFileProducer(conf.FilePath)
		if err != nil {
			return nil, errors.New(fmt.Sprintf(
				"err creating file producer: '%v'\n",
				err.Error(),
			))
		}
		break
	case "nsq":
		nsqConf := &nsqbus.Config{}
		if len(conf.NsqdHosts) == 0 {
			nsqConf.NSQdAddrs = []string{"localhost:4150"}
		} else {
			nsqConf.NSQdAddrs = conf.NsqdHosts
		}

		p = nsqbus.NewProducer(nsqConf)
		break
	default:
		return nil, errors.New(fmt.Sprintf(
			"task_bus '%v' not supported - choices are 'stdout', 'file' or 'nsq'",
			conf.TaskBus,
		))
	}

	return p, nil
}
