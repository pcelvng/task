package bus

import (
	"errors"
	"fmt"

	iobus "github.com/pcelvng/task/bus/io"
	"github.com/pcelvng/task/bus/nop"
	nsqbus "github.com/pcelvng/task/bus/nsq"
	"github.com/pcelvng/task/bus/pubsub"
)

const (
	defaultBus = "stdio"
)

var defaultNSQd = []string{"localhost:4150"}

func NewOptions(bus string) *Options {
	if bus == "" {
		bus = defaultBus
	}

	return &Options{
		Bus:       bus,
		NSQdHosts: defaultNSQd,
	}
}

// Options is a general config struct that
// provides all potential config values for all
// bus types.
type Options struct {
	// Possible Values:
	// - "stdio" (generic stdin, stdout)
	// - "stdin" (for consumer)
	// - "stdout" (for producer)
	// - "stderr" (for producer)
	// - "null" (for producer)
	// - "file"
	// - "nsq"
	// = "pubsub"
	// - "nop" - no-operation bus for testing
	Bus    string `toml:"bus" comment:"task message bus (nsq, pubsub, file, stdio)"`
	InBus  string `toml:"in_bus" commented:"true" comment:"set a different consumer bus type than producer (nsq, pubsub, file, stdin)"`
	OutBus string `toml:"out_bus" commented:"true" comment:"set a different producer bus type than consumer (nsq, pubsub, file, stdout, stderr, null)"`

	// consumer topic and channel
	InTopic   string `toml:"in_topic" commented:"true" comment:"for file bus in_topic is a file name"`
	InChannel string `toml:"in_channel" commented:"true" comment:"for pubsub this is the subscription name"`

	// for "nsq" bus type
	NSQdHosts    []string `toml:"nsqd_hosts" commented:"true" comment:"ndqd host names for producer or consumer"`
	LookupdHosts []string `toml:"lookupd_hosts" commented:"true" comment:"nsq lookupd host names consumer only"`

	// for "pubsub" bus type
	PubsubHost string `toml:"pubsub_host" commented:"true" comment:"pubsub host only for emulator"`
	ProjectID  string `toml:"project_id" commented:"true" comment:"pubsub goolge project name"`
	JSONAuth   string `toml:"json_auth" commented:"true" comment:"pubsub json data for authentication"`

	// NopMock for "nop" bus type,
	// Can be set in order to
	// mock various return scenarios.
	//
	// Supported Values:
	// - "init_err" - returns err on initialization: either NewProducer or NewConsumer
	// - "err" - every method returns an error
	// - "send_err" - returns err when Producer.Send() is called.
	// - "msg_err" - returns err on Consumer.Msg() call.
	// - "msg_done" - returns a nil task message done=true on Consumer.Msg() call.
	// - "msg_msg_done" - returns a non-nil task message and done=true Consumer.Msg() call.
	// - "stop_err" - returns err on Stop() method call
	NopMock string `toml:"-"`
}

// NewBus returns an instance of Bus.
func NewBus(opt *Options) (*Bus, error) {
	if opt == nil {
		opt = NewOptions("")
	}

	// make consumer
	c, err := NewConsumer(opt)
	if err != nil {
		return nil, err
	}

	// make producer
	p, err := NewProducer(opt)
	if err != nil {
		return nil, err
	}

	return &Bus{
		consumer: c,
		producer: p,
	}, nil
}

// Bus combines a consumer and producer into a single struct
// and implements both the Consumer and Producer interfaces.
//
// Bus is the same as getting a Consumer and Producer
// separately but having them available in a single object.
//
// Calling Stop() will stop the producer first then the consumer.
type Bus struct {
	consumer Consumer
	producer Producer
}

func (b *Bus) Msg() (msg []byte, done bool, err error) {
	return b.consumer.Msg()
}

func (b *Bus) Send(topic string, msg []byte) error {
	return b.producer.Send(topic, msg)
}

func (b *Bus) Stop() error {
	cErr := b.consumer.Stop()
	pErr := b.producer.Stop()

	if cErr != nil {
		return cErr
	}

	if pErr != nil {
		return pErr
	}

	return nil
}

// NewProducer creates a bus producer from Option.
func NewProducer(opt *Options) (Producer, error) {
	if opt == nil {
		opt = NewOptions("")
	}

	var p Producer
	var err error

	// normalize bus value
	busType := opt.Bus
	if opt.OutBus != "" {
		// out bus value override
		busType = opt.OutBus
	}

	switch busType {
	case "null":
		p = iobus.NewNullProducer()

	case "stderr":
		p = iobus.NewStdErrProducer()

	case "stdout", "stdio", "":
		p = iobus.NewStdoutProducer()

	case "file":
		p = iobus.NewProducer()

	case "nsq":
		nsqOpt := &nsqbus.Option{}
		if len(opt.NSQdHosts) == 0 {
			nsqOpt.NSQdAddrs = defaultNSQd
		} else {
			nsqOpt.NSQdAddrs = opt.NSQdHosts
		}

		p, err = nsqbus.NewProducer(nsqOpt)
	case "pubsub":
		psOpt := pubsub.NewOption(opt.PubsubHost, opt.ProjectID, opt.InChannel, opt.InTopic, opt.JSONAuth, 2)
		p, err = psOpt.NewProducer()

	case "nop":
		p, err = nop.NewProducer(opt.NopMock)

	default:
		err = errors.New(fmt.Sprintf(
			"task bus '%v' not supported",
			busType,
		))
	}

	if err != nil {
		return nil, errors.New(fmt.Sprintf(
			"new producer: '%v'\n",
			err.Error(),
		))
	}

	return p, nil
}

// NewConsumer creates a bus consumer from BusConfig.
func NewConsumer(opt *Options) (Consumer, error) {

	if opt == nil {
		opt = NewOptions("")
	}

	var c Consumer
	var err error

	// normalize bus value
	busType := opt.Bus
	if opt.InBus != "" {
		// in bus value override
		busType = opt.InBus
	}

	switch busType {
	case "stdin", "stdio", "":
		opt.InTopic = "/dev/stdin"
		fallthrough

	case "file":
		c, err = iobus.NewConsumer(opt.InTopic)

	case "nsq":
		nsqOpt := &nsqbus.Option{}
		if len(opt.LookupdHosts) > 0 {
			nsqOpt.LookupdAddrs = opt.LookupdHosts
		} else if len(opt.NSQdHosts) > 0 {
			nsqOpt.NSQdAddrs = opt.NSQdHosts
		} else {
			nsqOpt.NSQdAddrs = defaultNSQd
		}
		c, err = nsqbus.NewConsumer(opt.InTopic, opt.InChannel, nsqOpt)

	case "pubsub":
		psOpt := pubsub.NewOption(opt.PubsubHost, opt.ProjectID, opt.InChannel, opt.InTopic, opt.JSONAuth, 1)
		c, err = psOpt.NewConsumer()

	case "nop":
		c, err = nop.NewConsumer(opt.NopMock)

	default:
		err = errors.New(fmt.Sprintf(
			"task bus '%v' not supported",
			busType,
		))
	}

	if err != nil {
		return nil, errors.New(fmt.Sprintf(
			"new consumer: '%v'\n",
			err.Error(),
		))
	}

	return c, nil
}
