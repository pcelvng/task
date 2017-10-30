package task

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pcelvng/task"
)

var (
	defaultTimeout = time.Second * 10
)

func NewConfig() *Config {
	return &Config{
		MaxInFlight: 1,
		Timeout:     defaultTimeout,
	}
}

type Config struct {
	// MaxInFlight is the max number tasks
	// in progress at one time.
	MaxInFlight int

	// Timeout is how long the launcher will
	// wait for a forced-shutdown worker to
	// return a done task.
	Timeout time.Duration
}

// NewLauncher creates a Launcher and context
// for forcing the launcher to shutdown.
func NewLauncher(rcvr Receiver, lnchFn LaunchFunc, config *Config) (*Launcher, context.Context, error) {
	// create config if none provided
	if config == nil {
		config = NewConfig()
	}

	// make sure maxInFlight is at least 1
	maxInFlight := 1
	if config.MaxInFlight > 1 {
		maxInFlight = config.MaxInFlight
	}

	// use default timeout if none provided.
	timeout := defaultTimeout
	if config.Timeout > 0 {
		timeout = config.Timeout
	}

	// create max in flight slots
	slots := make(chan int, maxInFlight)
	for i := maxInFlight; i > 0; i-- {
		slots <- 1
	}

	// create context
	ctx, cncl := context.WithCancel(context.Background())

	return &Launcher{
		conf:         config,
		receiver:     rcvr,
		launchFunc:   lnchFn,
		ctx:          ctx,
		cncl:         cncl,
		maxInFlight:  maxInFlight,
		slots:        slots,
		closeTimeout: timeout,
		lastChan:     make(chan interface{}),
		closedChan:   make(chan interface{}),
		quitChan:     make(chan interface{}),
	}, ctx, nil
}

type Launcher struct {
	conf         *Config
	receiver     task.Receiver
	launchFunc   task.LaunchFunc
	ctx          context.Context
	cncl         context.CancelFunc
	closeTimeout time.Duration

	// wg is the wait group for communicating
	// when all tasks are complete.
	sync.WaitGroup

	// quitChan signals that everything needs
	// to be shutdown by the launcher.
	quitChan chan interface{}

	// closedChan chan interface{}
	// closing closedChan indicates that everything
	// has completed and/or shutdown in the best
	// way possible.
	// This is the mechanism that the launcher uses
	// to communicate to the application that everything
	// is done so that the application can shut down if
	// it wants to.
	closedChan chan interface{}
	isClosed   int64

	// marks that the Start() method has been called.
	started int64

	// doing records that the Do()
	// method has already been called.
	doing bool

	// describes the maximum number of tasks
	// that the launcher will allow at one
	// time.
	//
	// Must have a value greater than zero.
	maxInFlight int

	// slots describes the number of slots
	// available. Each slot represents a
	// worker opening to work on a task -
	// if a task is available.
	slots chan int

	// last will be set to true when it is processing
	// the last message. When the last message is
	// in progress the lastChan will be closed and when
	// the final task is complete the launcher will
	// close down.
	last     int64
	lastChan chan interface{}
}

// Start will connect the receiver and start
// the worker so that it is ready to accept tasks.
func (l *Launcher) Start() (chan interface{}, error) {
	// check if already started
	if !atomic.CompareAndSwapInt64(&l.started, 0, 1) {
		return l.closedChan, nil
	}

	closedChan := make(chan interface{})
	l.closedChan = closedChan

	// get the receiver ready
	if err := l.receiver.Connect(); err != nil {
		return l.closedChan, err
	}
	return l.closedChan, nil
}

// Do will get tasks from the receiver
// and pass the tasks to the worker.
//
// It goes without saying that Do needs
// to be called after Start
//
// Calling Do more than once is safe but
// will not do anything.
func (l *Launcher) Do() {
	if l.doing {
		return
	}
	go l.do()

	l.doing = true
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
	//
	// receivers are lazy - they lazy load
	// but workers are greedy - if there
	// is a task available a worker will
	// work on it right away. But a task
	// will only be requested if there
	// is a worker slot available to work on it.
	for {
		select {
		case <-l.quitChan:
			return
		case <-l.lastChan:
			// make sure call l.Wait()
			// before l.Close() or else
			// the tasks will be forced
			// to close without completing.
			l.Wait()
			l.Close()
			return

		// request another task
		// if there is a slot available
		case <-l.slots:
			l.next()
		}
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
	tsk, done, err := l.receiver.Next()

	// An error from next means that
	// Next was called when the queue
	// is empty and there are no more
	// tasks to complete.
	//
	// Log the message and send the 'last'
	// signal.
	if err != nil {
		log.Println(err.Error())

		// close up shop if all done.
		if done {
			if atomic.CompareAndSwapInt64(&l.last, 0, 1) {
				close(l.lastChan)
			}
		}

		// an error means there is no task so just return.
		l.giveBackSlot()
		return
	}

	// do task
	if tsk != nil {
		go l.doLaunch(tsk)
	}

	// close up shop if all done.
	if done {
		if atomic.CompareAndSwapInt64(&l.last, 0, 1) {
			close(l.lastChan)
		}
	}
}

// giveSlot will attempt to give back a slot.
// needs to be used with care to prevent locks.
func (l *Launcher) giveBackSlot() {
	l.slots <- 1
}

// doLaunch will safely handle the wait
// group and cleanly close down a worker
// and report back on the task result.
func (l *Launcher) doLaunch(tsk *task.Task) {
	l.Add(1)
	defer l.Done()

	worker := l.launchFunc(tsk)
	doneTsk := worker.DoTask()

	select {
	case tsk = <-doneTsk:
		// send task back to the receiver
		l.receiver.Done(tsk)

		// give back the slot so that the next task
		// can be launched.
		l.giveBackSlot()

		return
	case <-l.quitChan:
		// force the worker closed
		worker.Close()

		// wait for the task to close out
		// cleanly but only wait as long
		// as the timeout permits.
		tckr := time.NewTicker(l.closeTimeout)
		select {
		case tsk = <-doneTsk:
			tckr.Stop()
			l.receiver.Done(tsk)
			return
		case <-tckr.C:
			// only case where the
			// launcher will manage task state
			msg := fmt.Sprintf("worker closed from timeout; waited '%v'", l.closeTimeout.String())
			tsk.Err(msg)
			l.receiver.Done(tsk)
			return
		}
	}
}

// Close will close the receiver
// and outstanding workers and shutdown the do loop.
// Close is safe to call more than once and safe
// to call before Start but won't do anything in both
// cases.
func (l *Launcher) Close() error {
	// check that Start was called
	if atomic.LoadInt64(&l.started) != int64(1) {
		return nil
	}

	// check that Close hasn't already been called
	if !atomic.CompareAndSwapInt64(&l.isClosed, 0, 1) {
		return nil // already called
	}

	// close the task loop and shutdown
	// all workers. Give all the workers
	// a chance to close cleanly up until
	// the timeout is reached.
	close(l.quitChan)
	l.Wait()
	defer close(l.closedChan)

	// finally, close the receiver
	// all task responses should have already
	// processed.
	if err := l.receiver.Close(); err != nil {
		return err
	}

	return nil
}
