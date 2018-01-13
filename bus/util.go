package bus

import (
	"errors"
	"fmt"

	iobus "github.com/pcelvng/task/bus/io"
	"github.com/pcelvng/task/bus/nop"
	nsqbus "github.com/pcelvng/task/bus/nsq"
)

var (
	defaultBus       = "stdio"
	defaultReadPath  = "./in.tsks.json"
	defaultWritePath = "./out.tsks.json"
	defaultNSQd      = []string{"localhost:4150"}
)

func NewBusOpt(bus string) *BusOpt {
	if bus == "" {
		bus = defaultBus
	}

	return &BusOpt{
		Bus:       bus,
		InFile:    defaultReadPath,
		OutFile:   defaultWritePath,
		NsqdHosts: defaultNSQd,
	}
}

// BusOpt is a general config struct that
// provides all potential config values for all
// bus types.
type BusOpt struct {
	// Possible Values:
	// - "stdio" (generic stdin, stdout)
	// - "stdin" (for consumer)
	// - "stdout" (for producer)
	// - "file"
	// - "nsq"
	// - "nop" - no-operation bus for testing
	Bus    string `toml:"bus"`
	InBus  string `toml:"in_bus"`
	OutBus string `toml:"out_bus"`

	// for "file" bus type
	InFile  string `toml:"in_file"`  // file producer
	OutFile string `toml:"out_file"` // file consumer

	// for "nsq" bus type
	NsqdHosts    []string `toml:"nsqd_hosts"`    // nsq producer or consumer
	LookupdHosts []string `toml:"lookupd_hosts"` // nsq consumer only

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
	NopMock string

	// consumer topic and channel
	Topic   string `toml:"topic"`   // required for consumers
	Channel string `toml:"channel"` // required for consumers
}

// NewBus returns in instance of Bus.
func NewBus(opt *BusOpt) (*Bus, error) {
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
// and implements both the ConsumerBus and ProducerBus interfaces.
//
// Bus is the same as getting a Consumer and Producer
// separately but having them available in a single object.
//
// Calling Stop() will stop the producer first then the consumer.
type Bus struct {
	consumer ConsumerBus
	producer ProducerBus
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

// NewProducer creates a bus producer from BusOpt.
func NewProducer(opt *BusOpt) (ProducerBus, error) {
	var p ProducerBus
	var err error
	// normalize bus value
	busType := opt.Bus
	if opt.OutBus != "" {
		// out bus value override
		busType = opt.OutBus
	}

	switch busType {
	case "stdout", "stdio", "":
		p = iobus.NewStdoutProducer()
	case "file":
		writePath := opt.OutFile
		if writePath == "" {
			writePath = defaultWritePath
		}

		p, err = iobus.NewFileProducer(writePath)
	case "nsq":
		nsqOpt := &nsqbus.Opt{}
		if len(opt.NsqdHosts) == 0 {
			nsqOpt.NSQdAddrs = defaultNSQd
		} else {
			nsqOpt.NSQdAddrs = opt.NsqdHosts
		}

		p, err = nsqbus.NewProducer(nsqOpt)
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
func NewConsumer(opt *BusOpt) (ConsumerBus, error) {
	var c ConsumerBus
	var err error

	// normalize bus value
	busType := opt.Bus
	if opt.InBus != "" {
		// in bus value override
		busType = opt.InBus
	}

	switch busType {
	case "stdin", "stdio", "":
		c = iobus.NewStdinConsumer()
	case "file":
		readPath := opt.InFile
		if readPath == "" {
			readPath = defaultReadPath
		}

		c, err = iobus.NewFileConsumer(readPath)
	case "nsq":
		nsqOpt := &nsqbus.Opt{}
		if len(opt.LookupdHosts) > 0 {
			nsqOpt.LookupdAddrs = opt.LookupdHosts
		} else if len(opt.NsqdHosts) > 0 {
			nsqOpt.NSQdAddrs = opt.NsqdHosts
		} else {
			nsqOpt.NSQdAddrs = defaultNSQd
		}

		c, err = nsqbus.NewConsumer(opt.Topic, opt.Channel, nsqOpt)
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
