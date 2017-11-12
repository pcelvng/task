package bus

import (
	"errors"
	"fmt"

	iobus "github.com/pcelvng/task/bus/io"
	nsqbus "github.com/pcelvng/task/bus/nsq"
)

type Consumer interface {
	Msg() (msg []byte, done bool, err error)
	Stop() error
}

type Producer interface {
	Send(topic string, msg []byte) error
	Stop() error
}

var (
	defaultBus       = "stdio"
	defaultWritePath = "./out.tsks.json"
	defaultReadPath  = "./in.tsks.json"
	defaultNSQd      = []string{"localhost:4150"}
)

func NewBusesConfig(bus string) *BusesConfig {
	if bus == "" {
		bus = defaultBus
	}

	return &BusesConfig{
		Bus:       bus,
		NsqdHosts: defaultNSQd,
	}
}

// BusesConfig is a general config struct that
// provides all potential config values for all
// bus types.
//
// It can be used to easily and dynamically create
// a producer and/or consumer.
type BusesConfig struct {
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
}

// NewProducer creates a bus producer from BusesConfig.
func NewProducer(conf *BusesConfig) (Producer, error) {
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
		if err != nil {
			return nil, errors.New(fmt.Sprintf(
				"err creating file producer: '%v'\n",
				err.Error(),
			))
		}

		break
	case "nsq":
		nsqConf := &nsqbus.Config{}
		if len(conf.NsqdHosts) == 0 {
			nsqConf.NSQdAddrs = defaultNSQd
		} else {
			nsqConf.NSQdAddrs = conf.NsqdHosts
		}
		p = nsqbus.NewProducer(nsqConf)

		break
	default:
		return nil, errors.New(fmt.Sprintf(
			"task bus '%v' not supported - choices are 'stdio', 'stdout', 'file' or 'nsq'",
			conf.OutBus,
		))
	}

	return p, nil
}

// NewConsumer creates a bus consumer from BusesConfig.
func NewConsumer(conf *BusesConfig) (Consumer, error) {
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
		if err != nil {
			return nil, errors.New(fmt.Sprintf(
				"err creating file consumer: '%v'\n",
				err.Error(),
			))
		}

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
		c, err = nsqbus.NewLazyConsumer(nsqConf)

		break
	default:
		return nil, errors.New(fmt.Sprintf(
			"task bus '%v' not supported - choices are 'stdio', 'stdin', 'file' or 'nsq'",
			conf.InBus,
		))
	}

	return c, nil
}
