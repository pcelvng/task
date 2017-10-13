package nsqbus

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"

	nsq "github.com/bitly/go-nsq"
)

func NewLazyConsumer(c *LazyConsumerConfig) (*LazyConsumer, error) {
	// initialized at 0 to pause reading on startup
	maxInFlight := 0

	// create nsq consumer config
	nsqConf := nsq.NewConfig()
	nsqConf.MaxInFlight = maxInFlight

	lc := &LazyConsumer{
		conf:        c,
		nsqConf:     nsqConf,
		maxInFlight: int64(maxInFlight),
		closeChan:   make(chan interface{}),
		msgChan:     make(chan []byte),
	}

	return lc, nil
}

type LazyConsumerConfig struct {
	Topic        string
	Channel      string
	NSQdAddrs    []string // connects via TCP only
	LookupdAddrs []string // connects via HTTP only

	// if nil then the default nsq logger is used
	Logger *log.Logger

	// default is nsq.LogLevelInfo. Only set if a
	// custom logger is provided.
	LogLvl nsq.LogLevel
}

type LazyConsumer struct {
	conf     *LazyConsumerConfig
	nsqConf  *nsq.Config
	consumer *nsq.Consumer

	// maxInFlight is managed so that messages are lazy loaded
	maxInFlight int64

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
	consumer, err := nsq.NewConsumer(c.conf.Topic, c.conf.Channel, c.nsqConf)
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

// inMaxInFlight will increment the consumer maxInFlight
// value by 1
func (c *LazyConsumer) incMaxInFlight() {
	c.Lock()
	atomic.AddInt64(&c.maxInFlight, 1)
	c.consumer.ChangeMaxInFlight(int(c.maxInFlight))
	c.Unlock()
}

// decMaxInFlight will decrement the consumer maxInFlight
// value by 1
func (c *LazyConsumer) decMaxInFlight() {
	c.Lock()
	atomic.AddInt64(&c.maxInFlight, -1)
	c.consumer.ChangeMaxInFlight(int(c.maxInFlight))
	c.Unlock()
}

func (c *LazyConsumer) HandleMessage(msg *nsq.Message) error {
	// decrement maxInFlight on exit
	defer c.decMaxInFlight()

	body := msg.Body
	// the message should be ready to accept immediately
	// or else the calling application risks that the message
	// reaches the timeout limit and is re-queued.
	select {
	case c.msgChan <- body:
		return nil // successful - will ack the message as finished
	case <-c.closeChan:
		err := errors.New("nsq consumer shut down before the message was read in")
		return err
	}

	return nil
}

// Msg will block until it receives one and only one message.
//
// Msg is safe to call concurrently.
func (c *LazyConsumer) Msg() ([]byte, error) {
	// increment wait group
	c.Add(1)
	defer c.Done()

	c.incMaxInFlight()

	// wait for a message
	msgBytes := make([]byte, 0)
	select {
	case msgBytes = <-c.msgChan:
		return msgBytes, nil
	case <-c.closeChan:
		return nil, nil
	}

	return msgBytes, nil
}

// Close will close down all in-process activity
// and the nsq consumer.
func (c *LazyConsumer) Close() error {
	// close out all work in progress
	close(c.closeChan)
	c.Wait()

	// stop the consumer
	if c.consumer != nil {
		c.consumer.Stop()
		<-c.consumer.StopChan // wait for consumer to finish closing
	}

	return nil
}
