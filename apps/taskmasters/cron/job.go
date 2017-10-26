package main

import (
	"log"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/util"
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
	tskValue := util.FmtTask(j.TaskTemplate, offsetDate(j.HourOffset))
	tsk := task.New(j.TaskType, tskValue)
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

// offsetDate will return the time.Time value with the
// hour offset.
func offsetDate(offset int) time.Time {
	now := time.Now()
	return now.Add(time.Hour * time.Duration(offset))
}
