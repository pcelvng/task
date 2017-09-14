package launcher

import (
	"sync"

	"github.com/pcelvng/task"
)

type Launcher struct {
	conf       *Config
	receiver   task.Receiver
	launchFunc task.Launch

	// wg is the wait group for managing in-progress
	// tasks.
	sync.WaitGroup

	// quit signals that everything needs
	// to be shutdown by the launcher.
	quit chan int

	// describes the maximum number of tasks
	// that the launcher will allow at one
	// time.
	maxInFlight int

	// onLast is true when the last task in
	// a series of tasks has been received.
	// If onLast is true then the launcher will
	// no longer try to listen for more tasks
	// and will wait for all in-progress tasks
	// to finish. Once all tasks are finished
	// then the launcher will shutdown by calling
	// Close().
	onLast bool
}

type Config struct {
	Receiver    task.Receiver
	LaunchFunc  task.Launch
	MaxInFlight int
}

// New will create a new Launcher instance.
func New(c *Config) (*Launcher, error) {
	return &Launcher{
		conf:        c,
		receiver:    c.Receiver,
		launchFunc:  c.LaunchFunc,
		maxInFlight: c.MaxInFlight,
	}, nil
}

// Start will connect the receiver and start
// the worker so that it is ready to accept tasks.
func (l *Launcher) Start() error {
	// get the receiver ready
	if err := l.receiver.Connect(); err != nil {
		return err
	}

	return nil
}

// Do will get tasks from the receiver
// and pass the tasks to the worker.
//
// It goes without saying that Do needs
// to be called after Start
func (l *Launcher) Do() {
	go l.do()
}

// do will check if maxInFlight has been reached.
// If maxInFlight has not been reached then it listens
// for a task and when a task is received it will
// - reached maxInFlight?
//   - yes: wait for a task to complete then check again
//   - no: then get a new task
// - when a new task is received then send it
//   to the worker for completion.
// - if trying to get a task returns an error
//   log the error and try to get another task.
// - if a task is the last one then pass it on
//   to the worker, do not accept any more tasks
//   and when all the tasks have finished close
//   the launcher.
func (l *Launcher) do() {
	// task loop
	for {
		l.next()
	}
}

// next will handle processing the next
// task. Namely it will make sure that only
// the maximum possible number of tasks in
// progress does not exceed the maxInFlight
// value.
//
// If next receives an error from the receiver
// then it will log the error.
//
// If next gets an error from the receiver it will
// log the error wait for a minute and try getting
// another task.
//
// - a new task is allowed (maxInFlight not reached yet)
// - the last task has not already been received.
func (l *Launcher) next() {
	l.Add(2)
}

// Close will close the receiver
// and the worker and shutdown the do loop.
func (l *Launcher) Close() error {
	// close the worker first in case
	// something is being worked on
	// ... handle closing all in-progress
	// tasks...

	// close the do loop
	// ...close

	// finally close the receiver
	if err := l.receiver.Close(); err != nil {
		return err
	}

	return
}
