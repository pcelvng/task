package nsq

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	gonsq "github.com/nsqio/go-nsq"
	"github.com/pcelvng/task/bus/info"
)

func NewConsumer(topic, channel string, opt *Option) (*Consumer, error) {
	if opt == nil {
		opt = &Option{}
	}

	// initialized at 0 to pause reading on startup
	maxInFlight := 0

	// create nsq consumer config
	nsqConf := gonsq.NewConfig()
	nsqConf.MaxInFlight = maxInFlight
	//nsqConf.MaxAttempts = 65535
	nsqConf.MaxAttempts = 0 // unlimited attempts
	nsqConf.BackoffMultiplier = 0
	nsqConf.MaxBackoffDuration = 0
	nsqConf.DefaultRequeueDelay = time.Second * 2
	nsqConf.MaxRequeueDelay = time.Second * 6
	nsqConf.RDYRedistributeInterval = time.Millisecond * 100

	// create context for clean shutdown
	ctx, cncl := context.WithCancel(context.Background())

	// create lazy consumer
	c := &Consumer{
		opt:       opt,
		nsqConf:   nsqConf,
		closeChan: make(chan interface{}),
		msgChan:   make(chan []byte),
		ctx:       ctx,
		cncl:      cncl,
		info: info.Consumer{
			Bus:   "nsq",
			Topic: topic,
		},
	}
	if topic != channel {
		c.info.Channel = channel
	}
	// connect to nsqd
	err := c.connect(topic, channel)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Consumer) checkMaxInFlight(topic, channel string) {
	// consumer is consider locked up if
	// 1. channel has depth
	// 2. msqRequested > msqReceived // is waiting for a message
	// 3. prevReceived = msgReceived // no new message has come in time limit
	var prev int64
	for ; ; time.Sleep(10 * time.Second) {
		depth := getDepth(c.opt.LookupdAddrs, topic, channel)
		if depth <= 0 || c.msgRequested <= c.msgReceived {
			continue
		}

		if prev == c.msgReceived {
			//set maxinflight to 2, wait
			log.Println("lockup detected: maxInFlight set to 2")
			c.consumer.ChangeMaxInFlight(2)
		} else {
			prev = c.msgReceived
		}

	}
}

type Consumer struct {
	opt      *Option
	nsqConf  *gonsq.Config
	consumer *gonsq.Consumer

	// msgRequested and msgReceived is necessary for lazy
	// loading b/c of the delay in communicating with nsqd
	// the new RDY count.
	msgRequested int64 // total messages requested
	msgReceived  int64 // total messages received

	// bytes are assumed serializable as a valid Task
	// msgChan should be created with a consumer and should never be closed.
	msgChan chan []byte

	// close signal to shut down Consumer. Will stop any in-process messages
	// (messages are re-queued). Will also shutdown any open nsq consumers.
	closeChan chan interface{}

	// wait group is to make sure all consumers are closed during
	// a shutdown.
	sync.WaitGroup

	// mutex for updating consumer maxInFlight
	sync.Mutex

	// context for clean shutdown
	ctx  context.Context
	cncl context.CancelFunc

	info info.Consumer
}

// connect will connect nsq. An error is returned if there
// is a problem connecting.
func (c *Consumer) connect(topic, channel string) error {
	// initialize nsq consumer - does not connect
	consumer, err := gonsq.NewConsumer(topic, channel, c.nsqConf)
	if err != nil {
		return err
	}

	go c.checkMaxInFlight(topic, channel)

	// set custom logger
	if c.opt.Logger != nil {
		consumer.SetLogger(c.opt.Logger, c.opt.LogLvl)
	}

	consumer.AddHandler(c)

	// attempt to connect to lookupds (if provided)
	// or attempt to connect to nsqds (if provided)
	// if neither is provided attempt to connect to localhost
	if len(c.opt.LookupdAddrs) > 0 {
		err = consumer.ConnectToNSQLookupds(c.opt.LookupdAddrs)
		if err != nil {
			return err
		}
	} else if len(c.opt.NSQdAddrs) > 0 {
		err = consumer.ConnectToNSQDs(c.opt.NSQdAddrs)
		if err != nil {
			return err
		}
	} else {
		err = consumer.ConnectToNSQD("localhost:4150")
		if err != nil {
			return err
		}
	}

	// update nsq consumer
	c.consumer = consumer

	return nil
}

// reqMsg will un-pause messages in flight (if paused)
// (by setting maxInflight = 1)
func (c *Consumer) reqMsg() {
	c.Lock()
	// increase total message requested count
	atomic.AddInt64(&c.msgRequested, 1)

	// start flow of messages
	// can always  set the maxInFlight to 1
	// b/c nsq consumer will not update maxInFlight
	// if there is no change.
	c.consumer.ChangeMaxInFlight(1)

	c.Unlock()
}

// recMsg will attempt to 'receive' the message. If the
// message is received then 'true' is returned'. If the
// message could not be received (b/c all requested messages
// have been received) then 'false' is returned.
func (c *Consumer) recMsg() bool {
	c.Lock()
	defer c.Unlock()

	// check if all messages have been received
	if atomic.LoadInt64(&c.msgReceived) == atomic.LoadInt64(&c.msgRequested) {
		return false // all requested messages have been received
	}

	// increase total message received
	atomic.AddInt64(&c.msgReceived, 1)

	// pause flow if all requested messages have been received
	if atomic.LoadInt64(&c.msgReceived) == atomic.LoadInt64(&c.msgRequested) {
		// pause flow
		c.consumer.ChangeMaxInFlight(0)
	}

	return true // message was received
}

func (c *Consumer) HandleMessage(msg *gonsq.Message) error {
	msg.DisableAutoResponse()
	body := msg.Body

	// check if the message can be received
	// - we can get messages that were not requested
	// b/c of the delay in turning off the flow of messages.
	if !c.recMsg() {
		// re-queue message so that a ready worker somewhere else
		// can accept it
		msg.Requeue(-1)
		return nil
	}

	// the message should be ready to accept immediately
	// or else the calling application risks that the message
	// reaches the timeout limit and is re-queued.
	select {
	case <-c.ctx.Done():
		// requeue so message is not lost during shutdown
		msg.Requeue(-1)
		return nil
	case c.msgChan <- body:
		msg.Finish() // successful - ack message
		return nil
	}

	return nil
}

// Msg will block until it receives one and only one message.
//
// Safe to call concurrently. Safe to call after Stop() and if this
// is the case will simple return done=true.
func (c *Consumer) Msg() (msg []byte, done bool, err error) {
	if c.ctx.Err() != nil {
		// should not attempt to read if already stopped
		return msg, true, nil
	}

	c.reqMsg() // request message

	// wait for a message
	select {
	case msg = <-c.msgChan:
	case <-c.ctx.Done():
		done = true
	}
	c.info.Received++
	return msg, done, err
}

func (c *Consumer) Info() info.Consumer {
	return c.info
}

func (c *Consumer) Stop() error {
	if c.ctx.Err() != nil {
		return nil
	}

	c.cncl()

	// stop the consumer
	if c.consumer != nil {
		c.consumer.Stop()
		<-c.consumer.StopChan // wait for consumer to finish closing
	}

	return nil
}
