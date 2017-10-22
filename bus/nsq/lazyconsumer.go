package nsq

import (
	"sync"
	"sync/atomic"

	gonsq "github.com/bitly/go-nsq"
)

func NewLazyConsumer(c *Config) (*LazyConsumer, error) {
	// initialized at 0 to pause reading on startup
	maxInFlight := 0

	// create nsq consumer config
	nsqConf := gonsq.NewConfig()
	nsqConf.MaxInFlight = maxInFlight

	lc := &LazyConsumer{
		conf:      c,
		nsqConf:   nsqConf,
		closeChan: make(chan interface{}),
		msgChan:   make(chan []byte),
	}

	return lc, nil
}

type LazyConsumer struct {
	conf     *Config
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
}

// connect will connect nsq. An error is returned if there
// is a problem connecting.
func (c *LazyConsumer) Connect() error {
	// initialize nsq consumer - does not connect
	consumer, err := gonsq.NewConsumer(c.conf.Topic, c.conf.Channel, c.nsqConf)
	if err != nil {
		return err
	}

	// set custom logger
	if c.conf.Logger != nil {
		consumer.SetLogger(c.conf.Logger, c.conf.LogLvl)
	}

	consumer.AddHandler(c)

	// attempt to connect to lookupds (if provided)
	// or attempt to connect to nsqds (if provided)
	// if neither is provided attempt to connect to localhost
	if len(c.conf.LookupdAddrs) > 0 {
		err = consumer.ConnectToNSQLookupds(c.conf.LookupdAddrs)
		if err != nil {
			return err
		}
	} else if len(c.conf.NSQdAddrs) > 0 {
		err = consumer.ConnectToNSQDs(c.conf.NSQdAddrs)
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
func (c *LazyConsumer) reqMsg() {
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
func (c *LazyConsumer) recMsg() bool {
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

func (c *LazyConsumer) HandleMessage(msg *gonsq.Message) error {
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
	case <-c.closeChan:
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
// Msg is safe to call concurrently.
func (c *LazyConsumer) Msg() (msg []byte, done bool, err error) {
	c.reqMsg() // request message

	// wait for a message
	msg = <-c.msgChan

	return msg, done, err
}

// Close will close down all in-process activity
// and the nsq consumer.
func (c *LazyConsumer) Close() error {
	// close out all work in progress
	close(c.closeChan)

	// stop the consumer
	if c.consumer != nil {
		c.consumer.Stop()
		<-c.consumer.StopChan // wait for consumer to finish closing
	}

	return nil
}
