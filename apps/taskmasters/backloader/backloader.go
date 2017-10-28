package main

import (
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/util"
)

// NewBackloader will validate the config, create and connect the
// bus producer.
func NewBackloader(conf *Config) (*Backloader, error) {
	// validate config
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	// create producer and connect
	p, err := util.NewProducer(conf.BusesConfig)
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
		tskValue := util.FmtTask(bl.config.TaskTemplate, atHour)

		// create task
		tsk := task.New(bl.config.TaskType, tskValue)

		// normalize topic
		topic := bl.config.TaskType
		if bl.config.Topic != "" {
			topic = bl.config.Topic
		}

		// send task to task bus
		msg, err := tsk.Bytes()
		if err != nil {
			return cnt, err
		}

		if err := bl.busProducer.Send(topic, msg); err != nil {
			return cnt, err
		}
		cnt = cnt + 1

		// increment atHour by one hour
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
