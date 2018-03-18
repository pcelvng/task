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
	defaultWorkerKillTime      = time.Second * 10
	defaultDoneTopic           = "done"
	defaultMaxInProgress  uint = 1
)

// NewBusOptions is a convenience wrapper around
// bus.NewBusOptions. This way the user won't need to import
// another package for most use cases.
func NewBusOptions(busType string) *bus.Options {
	return bus.NewOptions(busType)
}

// NewBus is a convenience wrapper around
// bus.NewBus. This way the user won't need to import
// another package for most use cases.
func NewBus(conf *bus.Options) (*bus.Bus, error) {
	return bus.NewBus(conf)
}

// NewProducer is a convenience wrapper around
// bus.NewProducer. This way the user won't need to import
// another package for most use cases.
func NewProducer(conf *bus.Options) (bus.Producer, error) {
	return bus.NewProducer(conf)
}

// NewConsumer is a convenience wrapper around
// bus.NewConsumer. This way the user won't need to import
// another package for most use cases.
func NewConsumer(conf *bus.Options) (bus.Consumer, error) {
	return bus.NewConsumer(conf)
}

// LauncherOptions returns a new LauncherOptions.
func NewLauncherOptions(tskType string) *LauncherOptions {
	return &LauncherOptions{
		MaxInProgress:  defaultMaxInProgress,
		WorkerKillTime: defaultWorkerKillTime,
		DoneTopic:      defaultDoneTopic,
		TaskType:       tskType, // not required but highly encouraged
	}
}

// LauncherOptions contains the options for initializing a
// new Launcher. The default values will likely work for most cases.
type LauncherOptions struct {
	// MaxInProgress is the max number tasks
	// in progress at one time.
	MaxInProgress uint `toml:"max_in_progress" commented:"true"`

	// WorkerKillTime is how long the Launcher will
	// wait for a forced-shutdown worker to cleanup.
	WorkerKillTime time.Duration `toml:"worker_kill_time" commented:"true"`

	// LifetimeWorkers - maximum number of tasks the
	// Launcher will process before closing.
	//
	// The default value of 0 means there is no limit.
	LifetimeWorkers uint `toml:"lifetime_workers" commented:"true"`

	// DoneTopic - topic to publish to for done tasks.
	// Default: "done"
	DoneTopic string `toml:"done_topic" commented:"true"`

	// TaskType is highly encouraged to be provided. The task type is important for worker discovery and necessary
	// for expected functioning of the RejectBadType and IgnoreBadType options.
	// The default handling of a task with an non-matching task type is to create the worker anyway.
	TaskType string `toml:"-"`

	// RejectBadType will reject all task types that are not registered
	// with the Launcher with RegisterType.
	//
	// Note that if both RejectBadType and IgnoreBadType are true then the Launcher will
	// act as if only RejectBadType were true.
	RejectBadType bool `toml:"reject_bad_type" commented:'true' comment:"if true then unregistered task types are returned to the bus with an 'error' result and no worker is launched"`

	// RejectBadType will reject all task types that are not registered
	// with the Launcher with RegisterType.
	//
	// Note that if both RejectBadType and IgnoreBadType are true then the Launcher will
	// act as if only RejectBadType were true.
	IgnoreBadType bool `toml:"ignore_bad_type" commented:'true' comment:"if true then unregistered task types are ignored and no worker is launched`

	// custom logger option
	Logger *log.Logger `toml:"-"`
}

// NewLauncher creates a new Launcher.
func NewLauncher(mkr MakeWorker, opt *LauncherOptions, bOpt *bus.Options) (*Launcher, error) {
	if opt == nil {
		opt = NewLauncherOptions("")
	}

	if bOpt == nil {
		bOpt = NewBusOptions("")
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

	return NewLauncherFromBus(mkr, c, p, opt), nil
}

// NewLauncherFromBus returns a Launcher from the provided
// consumer and producer buses.
//
// Usually not necessary to use directly unless the caller
// is providing a non-standard library consumer, producer buses.
func NewLauncherFromBus(mke MakeWorker, c bus.Consumer, p bus.Producer, opt *LauncherOptions) *Launcher {
	// Launcher options
	if opt == nil {
		opt = NewLauncherOptions("")
	}
	if opt.DoneTopic == "" {
		opt.DoneTopic = defaultDoneTopic
	}

	lgr := log.New(os.Stderr, "", log.LstdFlags)
	if opt.Logger != nil {
		lgr = opt.Logger
	}

	// make sure maxInProgress is at least 1
	maxInProgress := uint(1)
	if opt.MaxInProgress > 1 {
		maxInProgress = opt.MaxInProgress
	}

	// use default timeout if none provided.
	workerTimeout := defaultWorkerKillTime
	if opt.WorkerKillTime > 0 {
		workerTimeout = opt.WorkerKillTime
	}

	// create max in progress slots
	slots := make(chan int, maxInProgress)
	for i := maxInProgress; i > 0; i-- {
		slots <- 1
	}

	// lifetime max remaining (0; no lifetime max)
	remaining := opt.LifetimeWorkers

	// doneCncl (done cancel function)
	// - is called internally by the Launcher to signal that the Launcher
	// has COMPLETED shutting down.
	//
	// doneCtx (done context)
	// - is for communicating externally that the Launcher is DONE and
	// has shutdown gracefully.
	doneCtx, doneCncl := context.WithCancel(context.Background())

	// stop context and cancel func: shutdown Launcher/workers
	//
	// stopCtx - Launcher will listen on stopCtx.Done() for external forced shutdown.
	// stopCncl - used externally of Launcher to initiate forced Launcher shutdown.
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

	// unmatching task type handling
	typeHandling := ""
	if opt.IgnoreBadType {
		typeHandling = "ignore"
	}
	if opt.RejectBadType {
		typeHandling = "reject"
	}

	// warn if typeHandling is set and
	// a task type is not provided.
	if typeHandling != "" && opt.TaskType == "" {
		lgr.Printf("NO WORKERS WILL BE LAUNCHED! task type handling is set to '%v' but no task type is provided", typeHandling)
	}

	return &Launcher{
		isInitialized: true,
		consumer:      c,
		producer:      p,
		opt:           opt,
		mke:           mke,
		lgr:           opt.Logger,
		taskType:      opt.TaskType,
		typeHandling:  typeHandling,
		doneCtx:       doneCtx,
		doneCncl:      doneCncl,
		stopCtx:       stopCtx,
		stopCncl:      stopCncl,
		lastCtx:       lastCtx,
		lastCncl:      lastCncl,
		maxInProgress: maxInProgress,
		remaining:     remaining,
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
// The calling routine can force the Launcher to shutdown by calling
// the cancelFunc and then listening on context.Done to know when
// the Launcher has shutdown gracefully.
//
// For an example worker application look in ./apps/workers/noop/main.go.
type Launcher struct {
	// will panic if not properly initialized with the NewLauncher function.
	isInitialized bool

	// isDoing indicates the Launcher has already launched the task loop
	isDoing bool

	opt          *LauncherOptions
	consumer     bus.Consumer
	producer     bus.Producer
	mke          MakeWorker // for creating new workers
	lgr          *log.Logger
	taskType     string // registered task type; used for identifying the worker and handling task types that do not match.
	typeHandling string // how to handle unmatching task types: one of "reject", "ignore"

	// communicating Launcher has finished shutting down
	doneCtx  context.Context    // Launcher context (highest level context)
	doneCncl context.CancelFunc // Launcher cancel func (calling it indicates the Launcher has cleanly closed up)

	// forcing workers/Launcher to shut down
	// all worker contexts inherit from stopCtx.
	stopCtx  context.Context    // for listening to Launcher shutdown signal. Initiates shutdown process.
	stopCncl context.CancelFunc // for telling the Launcher to shutdown. Initiates shutdown process. Shutdown is complete when doneCtx.Done() is closed

	// indicate the last task is in progress
	lastCtx  context.Context    // main loop will listen on lastCtx.Done() to know if the last task is in progress
	lastCncl context.CancelFunc // called to indicate the last task is in progress

	// closeTimeout tells the Launcher how long to wait
	// when forcing a task to close.
	closeTimeout time.Duration

	// completeTimeout for forcing a task to complete within a
	// certain amount of time or force it to close.
	// if value is not set then this feature is disabled
	// and the Launcher will wait indefinitely for a task
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
	// that the Launcher will allow at one
	// time.
	//
	// A value of 0 is set to 1. Will always have a value of at
	// least 1.
	maxInProgress uint

	// remaining is decremented every time a new task
	// is requested. When remaining reaches 0 the task
	// requested is marked as the last and when it finishes
	// the Launcher will shutdown.
	//
	// An initial value of 0 means there is no lifetime limit.
	remaining uint

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
// The Launcher assumes the producer and consumer
// are fully initialized when the Launcher is created.
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
		panic("launcher not initialized")
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

	// typeHandling: 'reject'
	if l.typeHandling == "reject" {
		msg := fmt.Sprintf("unexpected task type '%v' wanting '%v'", tsk.Type, l.taskType)
		tsk.End(ErrResult, msg)
		return
	}

	// typeHandling: 'ignore'
	if l.typeHandling == "ignore" {
		// do nothing, the task is not returned and a worker
		// is not created.
		return
	}

	worker := l.mke(tsk.Info)
	doneChan := make(chan interface{})
	go func() {
		result, msg := worker.DoTask(wCtx)
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
	l.producer.Send(l.opt.DoneTopic, tsk.JSONBytes())
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
	l.lgr.Println(msg)
}

// Err can be called after the Launcher has
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
