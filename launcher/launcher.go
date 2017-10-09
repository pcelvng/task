package launcher

import (
	"log"
	"sync"
	"time"

	"github.com/pcelvng/task"
)

var (
	defaultTimeout = time.Second * 10
)

type Launcher struct {
	conf         *Config
	receiver     task.Receiver
	launch       task.Launch
	closeTimeout time.Duration

	// wg is the wait group for communicating
	// when all tasks are complete.
	sync.WaitGroup

	// quit signals that everything needs
	// to be shutdown by the launcher.
	quit chan int

	// complete is the channel used to pass
	// completed tasks from the workers.
	complete chan *task.Task

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

	// sending on last will signal that the
	// receiver has sent the last task.
	// After the last task and any other
	// in-progress tasks are complete the launcher
	// will shutdown.
	last chan int
}

type Config struct {
	Receiver    task.Receiver
	Launch      task.Launch
	MaxInFlight int
	Timeout     time.Duration
}

// New will create a new Launcher instance.
func New(c *Config) (*Launcher, error) {
	// make sure MaxInFlight is at least 1
	maxInFlight := 1
	if c.MaxInFlight > 1 {
		maxInFlight = c.MaxInFlight
	}

	// use default timeout if none is provided.

	// create the slots by populating the
	// buffered channel.
	slots := make(chan int, maxInFlight)
	for i := maxInFlight; i > 0; i-- {
		slots <- 1
	}

	return &Launcher{
		conf:        c,
		receiver:    c.Receiver,
		launch:      c.Launch,
		maxInFlight: maxInFlight,
		slots:       slots,
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
		case <-l.last:
			// make sure call l.Wait()
			// before l.Close() or else
			// the tasks will be forced
			// to close without completing.
			l.Wait()
			l.Close()
		// request another task
		// if there is a slot available
		case <-l.slots:
			l.next()
		case <-l.quit:
			break
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
	tsk, last, err := l.receiver.Next()

	// An error from next means that
	// Next was called when the queue
	// is empty and there are no more
	// tasks to complete.
	//
	// Log the message and send the 'last'
	// signal.
	if err != nil {
		log.Print(err.Error())

		l.last <- 1

		// an error means there is no task so just return.
		return
	}

	// do task
	go l.doLaunch(tsk)

	// record if the last task is reached.
	if last {
		l.last <- 1
	}
}

// doLaunch will safely handle the wait
// group and cleanly close down a worker
// and report back on the task result.
func (l *Launcher) doLaunch(tsk *task.Task) {
	l.Add(1)
	defer l.Done()

	worker := l.launch(tsk)
	doneTsk := worker.DoTask()

	select {
	case tsk <- doneTsk:
		// send task back to the receiver
		l.receiver.Done(tsk)
		return
	case <-l.quit:
		// force the worker close
		worker.Close()

		// wait for the task to close out
		// cleanly but only wait as long
		// as the timeout permits.
		tckr := time.NewTicker(l.closeTimeout)
		select {
		case tsk <- doneTsk:
			tckr.Stop()
			l.receiver.Done(tsk)
			return
		case <-tckr.C:
			// only case where the
			// launcher will manage task state
			msg := "worker close timeout"
			tsk.Err(msg)
			l.receiver.Done(tsk)
			return
		}
	}
}

// Close will close the receiver
// and outstanding workers and shutdown the do loop.
func (l *Launcher) Close() error {
	// close the task loop and shutdown
	// all workers. Give all the workers
	// a chance to close cleanly up until
	// the timeout is reached.
	close(l.quit)
	l.Wait()

	// finally close the receiver
	// all task responses should have already
	// processed.
	if err := l.receiver.Close(); err != nil {
		return err
	}

	return
}
