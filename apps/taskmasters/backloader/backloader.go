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

	// create producer
	p, err := bus.NewProducer(conf.BusConfig)
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
	startHour := bl.config.Start
	atHour := bl.config.Start
	endHour := bl.config.End

	// define positive or negative incrementer
	// depending on the direction of start to end
	incrementer := 1
	diff := int(atHour.Sub(endHour))
	if diff > 0 {
		incrementer = -1
	}

	cnt := 0
	onHours := makeOnHrs(bl.config.OnHours, bl.config.OffHours)

	for {
		// check if current hour is eligible
		if onHours[atHour.Hour()] && checkEvery(startHour, atHour, bl.config.EveryXHours) {
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
		}

		// NOTE: This calculation is indented to INCLUDE the end date
		// since the increment occurs after the difference compare.

		// check if the loop is finished
		// this works since both beginning and end are truncated
		// by hour.
		diff := int(endHour.Sub(atHour))
		if diff == 0 {
			return cnt, nil
		}

		// increment atHour by one hour
		atHour = atHour.Add(time.Hour * time.Duration(incrementer))
	}
}

// checkEvery will check if the hour is on an hour that should not
// be skipped from a specified 'SkipXHours' config value.
func checkEvery(startDate, atDate time.Time, every int) bool {
	// every must not be 0
	if every == 0 {
		every = 1
	}

	diff := startDate.Sub(atDate)
	hrsDiff := int(diff / (time.Hour)) // assures a discrete hour value
	if (hrsDiff % every) == 0 {
		return true
	}

	return false
}

// makeOnHrs will reconcile the config OnHours and
// OffHours into one final 'onHours' value.
//
// If onHrs and offHrs are all false then all hours will be true.
// If onHours has some values then those values will be set to
// false if there is a corresponding offHrs value.
//
// Will not try to protect itself against panics. Expects both
// slices to have len == 24. Will not go beyond 24.
func makeOnHrs(onHrs, offHrs []bool) []bool {
	finalHrs := make([]bool, 24)

	// set initial 'on' hours. If none are specified will
	// set all to on (true).
	allOn := true
	for i := 0; i < 24; i++ {
		if onHrs[i] {
			allOn = false
			break
		}
	}
	for i := 0; i < 24; i++ {
		if allOn {
			finalHrs[i] = true
		} else {
			finalHrs[i] = onHrs[i]
		}
	}

	// 'subtract' off hours
	for i := 0; i < 24; i++ {
		if offHrs[i] {
			finalHrs[i] = false
		}
	}

	return finalHrs
}

func (bl *Backloader) Stop() error {
	if bl.busProducer != nil {
		err := bl.busProducer.Stop()
		if err != nil {
			return err
		}
	}

	return nil
}
