package bus

import (
	"errors"
	"fmt"

	iobus "github.com/pcelvng/task/bus/io"
	nsqbus "github.com/pcelvng/task/bus/nsq"
)

var (
	defaultBus       = "stdio"
	defaultReadPath  = "./in.tsks.json"
	defaultWritePath = "./out.tsks.json"
	defaultNSQd      = []string{"localhost:4150"}
)

func NewBusConfig(bus string) *BusConfig {
	if bus == "" {
		bus = defaultBus
	}

	return &BusConfig{
		Bus:       bus,
		InFile:    defaultReadPath,
		OutFile:   defaultWritePath,
		NsqdHosts: defaultNSQd,
	}
}

// BusConfig is a general config struct that
// provides all potential config values for all
// bus types and making it easy to configure the
// consumer and producer as separate types.
//
// It can be used to easily and dynamically create
// a producer and/or consumer.
type BusConfig struct {
	// Possible Values:
	// - "stdio" (generic stdin, stdout)
	// - "stdin" (for consumer)
	// - "stdout" (for producer)
	// - "file"
	// - "nsq"
	Bus    string `toml:"bus"`
	InBus  string `toml:"in_bus"`
	OutBus string `toml:"out_bus"`

	// for "file" bus type
	InFile  string `toml:"in_file"`  // file producer
	OutFile string `toml:"out_file"` // file consumer

	// for "nsq" bus type
	NsqdHosts    []string `toml:"nsqd_hosts"`    // nsq producer or consumer
	LookupdHosts []string `toml:"lookupd_hosts"` // nsq consumer only

	// consumer topic and channel
	Topic   string // required for consumers
	Channel string // required for consumers
}

// NewBus is a convenience method for making a DynamicBus that
// implements the Bus interface.
//
// Convenience wrapper around NewDynamicBus using BusConfig.
func NewBus(conf *BusConfig) (*DynamicBus, error) {
	// make consumer
	c, err := NewConsumer(conf)
	if err != nil {
		return nil, err
	}

	// make producer
	p, err := NewProducer(conf)
	if err != nil {
		return nil, err
	}

	return NewDynamicBus(c, p), nil
}

func NewDynamicBus(c Consumer, p Producer) *DynamicBus {
	b := &DynamicBus{
		consumer: c,
		producer: p,
	}

	return b
}

// DynamicBus implements the Bus interface and can
// be a convenient way to get all the features of a
// producer and consumer in one.
//
// DynamicBus is the same as getting a Consumer and Producer
// separately but having them available in a single object.
type DynamicBus struct {
	consumer Consumer
	producer Producer
}

func (b *DynamicBus) Msg() (msg []byte, done bool, err error) {
	return b.consumer.Msg()
}

func (b *DynamicBus) Send(topic string, msg []byte) error {
	return b.producer.Send(topic, msg)
}

func (b *DynamicBus) Stop() error {
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

// NewConsumerProducer is a convenience method for making a consumer and producer
// using the generic BusConfig.
func NewConsumerProducer(conf *BusConfig) (Consumer, Producer, error) {
	// make consumer
	c, err := NewConsumer(conf)
	if err != nil {
		return nil, nil, err
	}

	// make producer
	p, err := NewProducer(conf)
	if err != nil {
		return nil, nil, err
	}

	return c, p, nil
}

// NewProducer creates a bus producer from BusConfig.
func NewProducer(conf *BusConfig) (Producer, error) {
	var p Producer
	var err error

	// normalize bus value
	busType := conf.Bus
	if conf.OutBus != "" {
		// out bus value override
		busType = conf.OutBus
	}

	switch busType {
	case "stdout", "stdio", "":
		p = iobus.NewStdoutProducer()

		break
	case "file":
		writePath := conf.OutFile
		if writePath == "" {
			writePath = defaultWritePath
		}

		p, err = iobus.NewFileProducer(writePath)

		break
	case "nsq":
		nsqConf := &nsqbus.Config{}
		if len(conf.NsqdHosts) == 0 {
			nsqConf.NSQdAddrs = defaultNSQd
		} else {
			nsqConf.NSQdAddrs = conf.NsqdHosts
		}
		p, err = nsqbus.NewProducer(nsqConf)

		break
	default:
		err = errors.New(fmt.Sprintf(
			"task bus '%v' not supported - choices are 'stdio', 'stdout', 'file' or 'nsq'",
			busType,
		))
		break
	}

	if err != nil {
		return nil, errors.New(fmt.Sprintf(
			"err creating producer: '%v'\n",
			err.Error(),
		))
	}

	return p, nil
}

// NewConsumer creates a bus consumer from BusConfig.
func NewConsumer(conf *BusConfig) (Consumer, error) {
	var c Consumer
	var err error

	// normalize bus value
	busType := conf.Bus
	if conf.InBus != "" {
		// in bus value override
		busType = conf.InBus
	}

	switch busType {
	case "stdin", "stdio", "":
		c = iobus.NewStdinConsumer()
		break
	case "file":
		readPath := conf.InFile
		if readPath == "" {
			readPath = defaultReadPath
		}

		c, err = iobus.NewFileConsumer(readPath)
		break
	case "nsq":
		nsqConf := &nsqbus.Config{}
		if len(conf.LookupdHosts) > 0 {
			nsqConf.LookupdAddrs = conf.LookupdHosts
		} else if len(conf.NsqdHosts) > 0 {
			nsqConf.NSQdAddrs = conf.NsqdHosts
		} else {
			nsqConf.NSQdAddrs = defaultNSQd
		}
		c, err = nsqbus.NewLazyConsumer(conf.Topic, conf.Channel, nsqConf)
		break
	default:
		err = errors.New(fmt.Sprintf(
			"task bus '%v' not supported - choices are 'stdio', 'stdin', 'file' or 'nsq'",
			busType,
		))
	}

	if err != nil {
		return nil, errors.New(fmt.Sprintf(
			"err creating consumer: '%v'\n",
			err.Error(),
		))
	}

	return c, nil
}
