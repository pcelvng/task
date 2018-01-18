package task

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/pcelvng/task/bus"
)

var (
	defaultWorkerTimeout = time.Second * 10
	defaultDoneTopic     = "done"
)

// NewBusOpt is a convenience wrapper around
// bus.NewBusOpt. This way the user won't need to import
// another package for most use cases.
func NewBusOpt(busType string) *bus.BusOpt {
	return bus.NewBusOpt(busType)
}

// NewBus is a convenience wrapper around
// bus.NewBus. This way the user won't need to import
// another package for most use cases.
func NewBus(conf *bus.BusOpt) (*bus.Bus, error) {
	return bus.NewBus(conf)
}

// NewProducer is a convenience wrapper around
// bus.NewProducer. This way the user won't need to import
// another package for most use cases.
func NewProducer(conf *bus.BusOpt) (bus.ProducerBus, error) {
	return bus.NewProducer(conf)
}

// NewConsumer is a convenience wrapper around
// bus.NewConsumer. This way the user won't need to import
// another package for most use cases.
func NewConsumer(conf *bus.BusOpt) (bus.ConsumerBus, error) {
	return bus.NewConsumer(conf)
}

// NewLauncherOpt returns a new LauncherOpt.
func NewLauncherOpt() *LauncherOpt {
	return &LauncherOpt{
		MaxInProgress:      1,
		WorkerTimeout:      defaultWorkerTimeout,
		LifetimeMaxWorkers: 0, // not enabled by default
		DoneTopic:          defaultDoneTopic,
		TaskType:           "",
		Logger:             log.New(os.Stderr, "", log.LstdFlags),
	}
}

// LauncherOpt contains the options for initializing a
// new launcher.
type LauncherOpt struct {
	// MaxInProgress is the max number tasks
	// in progress at one time.
	MaxInProgress int `toml:"max_in_progress"`

	// WorkerTimeout is how long the launcher will
	// wait for a forced-shutdown worker to cleanup.
	WorkerTimeout time.Duration `toml:"worker_timeout"`

	// LifetimeMaxWorkers - maximum number of tasks the
	// launcher will process before closing.
	LifetimeMaxWorkers int `toml:"lifetime_max_workers"`

	// DoneTopic - topic to publish to for done tasks.
	// Default: "done"
	DoneTopic string `toml:"done_topic"`

	// TaskType will check that the received task type
	// matches TaskType and if not then will return the task
	// with a task type mismatch error.
	//
	// If TaskType is empty then check will be skipped.
	TaskType string `toml:"task_type"`

	// custom logger option
	Logger *log.Logger `toml:"-"`
}

// NewLauncher creates a new launcher.
func NewLauncher(worker Worker, opt *LauncherOpt, bOpt *bus.BusOpt) (*Launcher, error) {
	if opt == nil {
		opt = NewLauncherOpt()
	}

	if bOpt == nil {
		bOpt = NewBusOpt("")
	}

	// consumer
	c, err := NewConsumer(bOpt)
	if err != nil {
		return nil, err
	}

	// producer
	p, err := NewProducer(bOpt)
	if err != nil {
		return nil, err
	}

	return NewLauncherFromBus(worker, c, p, opt), nil
}

// NewLauncherFromBus returns a Launcher from the provided
// consumer and producer buses.
func NewLauncherFromBus(worker Worker, c bus.ConsumerBus, p bus.ProducerBus, opt *LauncherOpt) *Launcher {
	// launcher options
	if opt == nil {
		opt = NewLauncherOpt()
	}

	// make sure maxInProgress is at least 1
	maxInProgress := 1
	if opt.MaxInProgress > 1 {
		maxInProgress = opt.MaxInProgress
	}

	// use default timeout if none provided.
	workerTimeout := defaultWorkerTimeout
	if opt.WorkerTimeout > 0 {
		workerTimeout = opt.WorkerTimeout
	}

	// create max in progress slots
	slots := make(chan int, maxInProgress)
	for i := maxInProgress; i > 0; i-- {
		slots <- 1
	}

	// doneCncl (done cancel function)
	// - is called internally by the launcher to signal that the launcher
	// has COMPLETED shutting down.
	//
	// doneCtx (done context)
	// - is for communicating externally that the launcher is DONE and
	// has shutdown gracefully.
	doneCtx, doneCncl := context.WithCancel(context.Background())

	// stop context and cancel func: shutdown launcher/workers
	//
	// stopCtx - launcher will listen on stopCtx.Done() for external forced shutdown.
	// stopCncl - used externally of launcher to initiate forced launcher shutdown.
	stopCtx, stopCncl := context.WithCancel(context.Background())

	// last context and cancel func - for indicating the last task
	// is in progress.
	//
	// lastCtx - for communicating that the last message has been
	// received and is currently being processed.
	//
	// lastCncl - for sending a signal indicating the last task has
	// been received and is currently being processed.
	lastCtx, lastCncl := context.WithCancel(context.Background())

	// make sure logger is not nil
	if opt.Logger == nil {
		opt.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	return &Launcher{
		isInitialized: true,
		consumer:      c,
		producer:      p,
		opt:           opt,
		worker:        worker,
		logger:        opt.Logger,
		doneCtx:       doneCtx,
		doneCncl:      doneCncl,
		stopCtx:       stopCtx,
		stopCncl:      stopCncl,
		lastCtx:       lastCtx,
		lastCncl:      lastCncl,
		maxInProgress: maxInProgress,
		slots:         slots,
		closeTimeout:  workerTimeout,
	}
}

// Launcher handles the heavy lifting of worker lifecycle, general
// task management and interacting with the bus.
//
// The calling routine should listen on context.Done to know if
// the Launcher has shut itself down.
//
// The calling routine can force the launcher to shutdown by calling
// the cancelFunc and then listening on context.Done to know when
// the Launcher has shutdown gracefully.
//
// For an example worker application look in ./apps/workers/noop/main.go.
type Launcher struct {
	// will panic if not properly initialized with the NewLauncher function.
	isInitialized bool

	// isDoing indicates the launcher has already launched the task loop
	isDoing bool

	opt      *LauncherOpt
	consumer bus.ConsumerBus
	producer bus.ProducerBus
	worker   Worker
	logger   *log.Logger

	// communicating launcher has finished shutting down
	doneCtx  context.Context    // launcher context (highest level context)
	doneCncl context.CancelFunc // launcher cancel func (calling it indicates the launcher has cleanly closed up)

	// forcing workers/launcher to shut down
	// all worker contexts inherit from stopCtx.
	stopCtx  context.Context    // for listening to launcher shutdown signal. Initiates shutdown process.
	stopCncl context.CancelFunc // for telling the launcher to shutdown. Initiates shutdown process. Shutdown is complete when doneCtx.Done() is closed

	// indicate the last task is in progress
	lastCtx  context.Context    // main loop will listen on lastCtx.Done() to know if the last task is in progress
	lastCncl context.CancelFunc // called to indicate the last task is in progress

	// closeTimeout tells the launcher how long to wait
	// when forcing a task to close.
	closeTimeout time.Duration

	// completeTimeout for forcing a task to complete within a
	// certain amount of time or force it to close.
	// if value is not set then this feature is disabled
	// and the launcher will wait indefinitely for a task
	// to complete.
	//
	// if the completeTimeout is reached then the task is forced
	// to close and will wait closeTimeout long before returning
	// the task.
	completeTimeout time.Duration

	// wg is the wait group for communicating
	// when all tasks are complete or have been
	// shutdown.
	wg sync.WaitGroup

	// maxInProgress describes the maximum number of tasks
	// that the launcher will allow at one
	// time.
	//
	// Must have a value greater than zero.
	maxInProgress int

	// remaining is decremented every time a new task
	// is requested. When remaining reaches 0 the task
	// requested is marked as the last and when it finishes
	// the launcher will shutdown.
	//
	// If remaining is not set or set to a negative number
	// then the launcher will not use a lifetime limit.
	remaining int

	// slots describes the number of slots
	// available. Each slot represents a
	// worker opening to work on a task -
	// if a task is available.
	slots chan int

	// closeErr is potentially set on shutdown
	// if there was an err to communicate after
	// shutdown is complete.
	closeErr error
	mu       sync.Mutex // managing safe access to closeErr
}

// DoTasks will start the task loop and immediately
// begin working on tasks if any are available.
//
// The launcher assumes the producer and consumer
// are fully initialized when the launcher is created.
//
// Will panic if not initialized with either NewLauncher
// or NewCPLauncher.
//
// Calling DoTasks more than once is safe but
// will not do anything. If called more than once will
// return a copy of the same context and cancel function
// received the first time.
func (l *Launcher) DoTasks() (doneCtx context.Context, stopCncl context.CancelFunc) {
	if !l.isInitialized {
		panic("launcher not correctly initialized!")
	}

	if l.isDoing {
		return l.doneCtx, l.stopCncl
	}
	go l.do()

	l.isDoing = true

	return l.doneCtx, l.stopCncl
}

// do is the main task loop.
func (l *Launcher) do() {
	defer l.doneCncl()

	for {
		select {
		case <-l.stopCtx.Done():
			goto Shutdown
		case <-l.lastCtx.Done():
			goto Shutdown
		case <-l.slots:
			l.wg.Add(1)
			l.mu.Lock()
			if l.remaining != 0 {
				if l.remaining > 0 {
					l.remaining = l.remaining - 1
				}
				if l.remaining == 0 {
					// the message about to be requested will
					// be the last one.
					l.lastCncl()
				}
			}
			l.mu.Unlock()

			// next() needs to be non-blocking so
			// the application can shut down when asked to.
			go l.next()
		}
	}

Shutdown:
	// close the consumer
	if err := l.consumer.Stop(); err != nil {
		l.mu.Lock()
		l.closeErr = err
		l.mu.Unlock()
	}

	// wait for workers to close up and send
	// task responses.
	l.wg.Wait()

	// stop the producer
	if err := l.producer.Stop(); err != nil {
		l.mu.Lock()
		l.closeErr = err
		l.mu.Unlock()
	}
}

// next handles getting and processing the next task.
func (l *Launcher) next() {
	tskB, done, err := l.consumer.Msg()
	if done {
		l.lastCncl()
	}
	if err != nil {
		l.log(err.Error())
		l.giveBackSlot()

		return
	}

	// handle a zero byte message
	if len(tskB) == 0 {
		l.giveBackSlot()

		return
	}

	tsk, err := NewFromBytes(tskB)
	if err != nil {
		l.log(err.Error())
		l.giveBackSlot()

		return
	}

	// launch worker and do task
	if tsk != nil {
		go l.doLaunch(tsk)
	}
}

// doLaunch will safely handle the wait
// group and cleanly close down a worker
// and report back on the task result.
func (l *Launcher) doLaunch(tsk *Task) {
	defer l.giveBackSlot()

	var wCtx context.Context
	var cncl context.CancelFunc
	if l.completeTimeout > time.Duration(0) {
		wCtx, cncl = context.WithTimeout(l.stopCtx, l.completeTimeout)
	} else {
		wCtx, cncl = context.WithCancel(l.stopCtx)
	}
	defer cncl() // clean up worker context

	// start task, after starting should always send back.
	tsk.Start()
	defer l.sendTsk(tsk)

	// check task type (if TaskType specified)
	if l.opt.TaskType != "" && l.opt.TaskType != tsk.Type {
		msg := fmt.Sprintf("wrong task type; expected '%v'", l.opt.TaskType)
		tsk.End(ErrResult, msg)
		return
	}

	doneChan := make(chan interface{})
	go func() {
		result, msg := l.worker.DoTask(wCtx, tsk.Info)
		tsk.End(result, msg)
		close(doneChan)
	}()

	select {
	case <-doneChan:
		break
	case <-wCtx.Done():
		// let worker clean up.
		tckr := time.NewTicker(l.closeTimeout)
		select {
		case <-doneChan:
			tckr.Stop()

			break
		case <-tckr.C:
			msg := fmt.Sprintf("worker forced to close; waited '%v'", l.closeTimeout.String())
			tsk.End(ErrResult, msg)

			break
		}
		break
	}
	return
}

func (l *Launcher) sendTsk(tsk *Task) {
	tskB, err := tsk.JSONBytes() // End() should already be called
	if err != nil {
		l.log(err.Error())
	} else {
		l.producer.Send(l.opt.DoneTopic, tskB)
	}
}

// giveSlot will attempt to give back a slot.
// needs to be used with correct accounting
// practices or will lock up.
//
// will not give back the slot if the application is
// shutting down or processing the last task.
func (l *Launcher) giveBackSlot() {
	if l.stopCtx.Err() == nil && l.lastCtx.Err() == nil {
		l.slots <- 1
	}
	l.wg.Done()
}

// log is the central point of operational logging.
func (l *Launcher) log(msg string) {
	l.logger.Println(msg)
}

// Err can be called after the launcher has
// communicated it has finished shutting down.
//
// If it's called before shutdown then will return
// nil. Will return the same error on subsequent
// calls.
func (l *Launcher) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.doneCtx.Err() != nil {
		return l.closeErr
	}

	return nil
}
