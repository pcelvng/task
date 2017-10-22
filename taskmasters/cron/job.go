package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
)

func NewJob(r *Rule, p bus.Producer) *Job {
	return &Job{
		Rule:     r,
		producer: p,
	}
}

type Job struct {
	*Rule
	producer bus.Producer
}

func (j *Job) Run() {
	// create Task
	tsk := task.New(j.TaskType, j.fmtTask())
	topic := j.TaskType
	if j.Topic != "" {
		topic = j.Topic
	}

	b, err := tsk.Bytes()
	if err != nil {
		log.Printf("err creating json bytes: '%v'", err.Error())
	}

	j.producer.Send(topic, b)
}

func (j *Job) fmtTask() string {
	now := time.Now()

	// adjust with offset
	offsetTime := now.Add(time.Hour * time.Duration(j.HourOffset))

	// substitute year {yyyy}
	y := strconv.Itoa(offsetTime.Year())
	s := strings.Replace(j.TaskFormat, "{yyyy}", y, -1)

	// substitute year {yy}
	s = strings.Replace(s, "{yy}", y[2:], -1)

	// substitute month {mm}
	m := fmt.Sprintf("%02d", int(offsetTime.Month()))
	s = strings.Replace(s, "{mm}", m, -1)

	// substitute day {dd}
	d := fmt.Sprintf("%02d", offsetTime.Day())
	s = strings.Replace(s, "{dd}", d, -1)

	// substitute hour {hh}
	h := fmt.Sprintf("%02d", offsetTime.Hour())
	s = strings.Replace(s, "{hh}", h, -1)

	return s
}
