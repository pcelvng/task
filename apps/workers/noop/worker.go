package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/pcelvng/task"
)

var config Config

func MakeWorker(info string) task.Worker {
	return &NoopWorker{
		info: info,
	}
}

type NoopWorker struct {
	info string
}

func (w *NoopWorker) DoTask(ctx context.Context) (result task.Result, msg string) {
	doneChan := make(chan interface{})
	go func() {
		result, msg = w.doTask()
		close(doneChan)
	}()

	select {
	case <-doneChan:
	case <-ctx.Done():
		result = task.ErrResult
		msg = "task interrupted"
	}

	return result, msg
}

func (w *NoopWorker) doTask() (task.Result, string) {
	// calc if failure
	isFail := checkFail(config.FailRate)

	var dur time.Duration
	if isFail { // calc failDuration
		dur = failDuration(config.Dur, config.DurVariance)
	} else {
		dur = successDuration(config.Dur, config.DurVariance)
	}

	// wait for duration
	time.Sleep(dur)

	// complete task and return
	if isFail {
		return task.ErrResult, "failed to complete"
	} else {
		return task.CompleteResult, "completed successfully"
	}
}

// successDuration will calculate how long the task
// will take to complete based on up to a random
// amount of variance.
//
// NOTE: the variance always adds to the base duration.
func successDuration(dur, durV time.Duration) time.Duration {
	// no variance so it's just the duration
	if durV == 0 {
		return dur
	}

	// generate a random variance
	seed := int64(time.Now().Nanosecond())
	randomizer := rand.New(rand.NewSource(seed))
	v := randomizer.Int63n(int64(durV))
	return dur + time.Duration(v)
}

// failDuration is any value up to the successDuration
// because it can fail at any time.
func failDuration(dur, durV time.Duration) time.Duration {
	maxDur := successDuration(dur, durV)
	if maxDur == 0 {
		return maxDur
	}

	// generate a random variance
	seed := int64(time.Now().Nanosecond())
	randomizer := rand.New(rand.NewSource(seed))
	v := randomizer.Int63n(int64(maxDur))
	return time.Duration(v)
}

// checkFail will return true if the task should
// be completed as an error and false otherwise.
// rate is assumed to be a value between 0-100.
// A value of 100 or more will always return true and
// a value of 0 or less will always return false.
func checkFail(rate int) bool {
	if rate <= 0 {
		return false
	}

	seed := int64(time.Now().Nanosecond())
	randomizer := rand.New(rand.NewSource(seed))
	if randomizer.Intn(100) <= rate {
		return true
	}

	return false
}
