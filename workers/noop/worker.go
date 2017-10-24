package main

import (
	"math/rand"
	"time"

	"sync"

	"github.com/pcelvng/task"
)

const workerType = "noop"

func NewLauncherFunc(c Config) task.LaunchFunc {
	return *LauncherFunc{
		conf: c,
	}
}

type LauncherFunc struct {
	conf Config
}

func (l *LauncherFunc) LaunchFunc(tsk *task.Task) task.Worker {
	return &NoopWorker{
		config:  l.conf,
		tsk:     tsk,
		tskChan: make(chan *task.Task),
	}
}

type NoopWorker struct {
	config  Config
	tsk     *task.Task
	tskChan chan *task.Task
	isDoing bool
	sync.Mutex
}

func (w *NoopWorker) DoTask() chan *task.Task {
	w.Lock()
	defer w.Unlock()

	if w.isDoing {
		return w.tskChan
	}
	go w.doTask()
	return w.tskChan
}

func (w *NoopWorker) doTask() {
	// calc if failure
	isFail = isFail(w.config.FailRate)

	var dur time.Duration
	if isFail { // calc failDuration
		dur = failDuration(w.config.Dur, w.config.DurVariance)
	} else {
		dur = successDuration(w.config.Dur, w.config.DurVariance)
	}
	w.tsk.Start()

	// wait for duration
	time.Sleep(dur)

	// complete task and return
	if isFail {
		w.tsk.Err("task failed")
	} else {
		w.tsk.Complete("task successfull")
	}

	w.tskChan <- w.tsk
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

	// generate a random variance
	seed := int64(time.Now().Nanosecond())
	randomizer := rand.New(rand.NewSource(seed))
	v := randomizer.Int63n(int64(maxDur))
	return time.Duration(v)
}

// isFail will return true if the task should
// be completed as an error and false otherwise.
// rate is assumed to be a value between 0-100.
// A value of 100 or more will always return true and
// a value of 0 or less will always return false.
func isFail(rate int) bool {
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

func (w *NoopWorker) Close() error {
	return nil
}

func (w *NoopWorker) WorkerType() string {
	return workerType
}
