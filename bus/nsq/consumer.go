package nsqbus

import (
	"errors"
	"sync"

	nsq "github.com/bitly/go-nsq"
)

func NewConsumer(c *ConsumerConfig) *Consumer {
	// create nsq consumer config
	nsqConf := nsq.NewConfig()
	nsqConf.MaxInFlight = 1 // 1 to prevent greedy reading

	return &Consumer{
		conf:    c,
		nsqConf: nsqConf,
	}
}

type ConsumerConfig struct {
	Topic            string
	Channel          string
	NSQdTCPAddrs     []string
	LookupdHTTPAddrs []string
}

type Consumer struct {
	conf    *ConsumerConfig
	nsqConf *nsq.Config

	// bytes are assumed serializable as a valid Task
	// msgChan should be created with a consumer and should never be closed.
	msgChan chan []byte

	// close signal to shut down Consumer. Will stop any in-process messages
	// (messages are re-queued). Will also shutdown any open nsq consumers.
	closeChan chan int

	// wait group is to make sure all consumers are closed during
	// a shutdown.
	sync.WaitGroup
}

// Connect will connect nsq. An error is returned if there
// is a problem connecting.
// - will create a one-time-use consumer that will disconnect
//   after reading exactly one message.
//
// Performance Note: if the ecosystem has a lot of nsqd instances this may take much more time.
// thus it is recommended to have an nsq topology that does not have too many instances
// of nsqd registered with the lookupds.
func (c *Consumer) Connect() (*nsq.Consumer, error) {
	// initialize nsq consumer - does not connect
	consumer, err := nsq.NewConsumer(c.conf.Topic, c.conf.Channel, c.nsqConf)
	if err != nil {
		return nil, err
	}

	// add handler - only good until the consumer is closed.
	// the way this is implemented the consumer is closed after 1
	// message is received to prevent a greedy consumer.
	consumer.AddHandler(c)

	// attempt to connect to nsqds (if provided)
	err = consumer.ConnectToNSQDs(c.conf.NSQdTCPAddrs)
	if err != nil {
		return nil, err
	}

	// attempt to connect to lookupds (if provided)
	err = consumer.ConnectToNSQLookupds(c.conf.LookupdHTTPAddrs)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func (c *Consumer) HandleMessage(msg *nsq.Message) error {
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
// To prevent getting more than one message at a time Msg will
// initialize a new nsq consumer and then shut the consumer down
// after one message is received.
//
// Msg is safe to call concurrently. Note that each concurrent call
// will still setup and take down a consumer.
func (c *Consumer) Msg() ([]byte, error) {
	// increment wait group
	c.Add(1)
	defer c.Done()

	consumer, err := c.Connect()
	if err != nil {
		return nil, err
	}

	// wait for a message
	// Note: all messages are passed on the same
	// channel so if Msg is called more than once
	// then the message that comes in could be from
	// a different consumer instance. This shouldn't
	// really be a problem. It just means that an nsq
	// consumer could get created and not actually read
	// in a message before being shut down. Then the
	// consumer that read in the first message will
	// continue waiting until another message is read
	// in.
	//
	// The important thing is to have message control.
	// A single message per Msg call.
	msgBytes := make([]byte, 0)
	select {
	case msgBytes = <-c.msgChan:
		break
	case <-c.closeChan:
		break
	}

	// close down the consumer - don't use this consumer again.
	consumer.Stop()

	// receive on StopChan to wait until consumer is completely shutdown
	// will make sure in-flight messages are re-queued cleanly.
	<-consumer.StopChan

	return msgBytes, nil
}

// Close will close down all in-process activity.
// Consumer should not be used after calling Close.
//
// Calling Close a second time will cause a panic.
func (c *Consumer) Close() error {
	close(c.closeChan)
	c.Wait()

	return nil
}
