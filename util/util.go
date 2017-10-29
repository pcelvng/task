package util

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pcelvng/task/bus"
	iobus "github.com/pcelvng/task/bus/io"
	nsqbus "github.com/pcelvng/task/bus/nsq"
)

var (
	defaultWritePath = "./out.tsks.json"
	defaultReadPath  = "./in.tsks.json"
)

func NewBusesConfig(bus string) *BusesConfig {
	return &BusesConfig{
		Bus: bus, // optional bus type
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
	InFile  string `toml:"in_file"`  // for file producer
	OutFile string `toml:"out_file"` // for file consumer

	// for "nsq" bus type
	NsqdHosts    []string `toml:"nsqd_hosts"`    // for producer or consumer
	LookupdHosts []string `toml:"lookupd_hosts"` // for consumer only
}

// NewProducer will create a producer dynamically according to
// the value of conf.BusType.
//
// BusType supported values: (see BusesConfig)
//
// For 'nsq' bus:
// - if no NsqdHosts are provided then it will default to 'localhost:4150'
func NewProducer(conf *BusesConfig) (bus.Producer, error) {
	// setup bus producer
	var p bus.Producer
	var err error

	// normalize bus type
	busType := conf.Bus
	if conf.OutBus != "" {
		// out bus type overrides generic bus type
		busType = conf.OutBus
	}

	switch busType {
	case "stdout", "stdio", "":
		p = iobus.NewStdoutProducer()
		log.Println("created stdout producer")

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
		log.Printf("created file producer at '%v'\n", writePath)

		break
	case "nsq":
		nsqConf := &nsqbus.Config{}
		if len(conf.NsqdHosts) == 0 {
			nsqConf.NSQdAddrs = []string{"localhost:4150"}
		} else {
			nsqConf.NSQdAddrs = conf.NsqdHosts
		}
		p = nsqbus.NewProducer(nsqConf)

		log.Printf("created nsq producer at '%v'\n", nsqConf.NSQdAddrs)

		break
	default:
		return nil, errors.New(fmt.Sprintf(
			"task bus '%v' not supported - choices are 'stdout', 'file' or 'nsq'",
			conf.OutBus,
		))
	}

	return p, nil
}

// NewConsumer will create a consumer dynamically according to
// the value of conf.BusType.
//
// BusType supported values (see BusesConfig)
//
// For 'nsq' bus:
// - if no NsqdHosts or LookupdHosts are provided then it
//   will default to 'localhost:4150'
func NewConsumer(conf *BusesConfig) (bus.Consumer, error) {
	// setup bus producer
	var c bus.Consumer
	var err error

	// normalize bus type
	busType := conf.Bus
	if conf.InBus != "" {
		// out but type overrides generic bus type
		busType = conf.InBus
	}

	switch busType {
	case "stdin", "stdio", "":
		c = iobus.NewStdinConsumer()
		log.Println("created stdin consumer")

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
		log.Printf("created file consumer at '%v'\n", readPath)

		break
	case "nsq":
		nsqConf := &nsqbus.Config{}
		if len(conf.LookupdHosts) > 0 {
			nsqConf.LookupdAddrs = conf.LookupdHosts
		} else if len(conf.NsqdHosts) > 0 {
			nsqConf.NSQdAddrs = conf.NsqdHosts
		} else {
			nsqConf.NSQdAddrs = []string{"localhost:4150"}
		}

		c, err = nsqbus.NewLazyConsumer(nsqConf)
		log.Printf("created nsq consumer at '%v%v'\n", nsqConf.NSQdAddrs, nsqConf.LookupdAddrs)

		break
	default:
		return nil, errors.New(fmt.Sprintf(
			"task bus '%v' not supported - choices are 'stdin', 'file' or 'nsq'",
			conf.InBus,
		))
	}

	return c, nil
}

// FmtTask will format a task value from 'template' and 'dateHour'.
// It supports the following format values:
//
// {yyyy} (year - four digits: ie 2017)
// {yy}   (year - two digits: ie 17)
// {mm}   (month - two digits: ie 12)
// {dd}   (day - two digits: ie 13)
// {hh}   (hour - two digits: ie 00)
//
// Example:
//
// template == "{yyyy}-{mm}-{dd}T{hh}:00"
//
// could return: "2017-01-01T23:00"
//
// If that was the corresponding time.Time value of dateHour.
func FmtTask(template string, dateHour time.Time) string {

	// substitute year {yyyy}
	y := strconv.Itoa(dateHour.Year())
	s := strings.Replace(template, "{yyyy}", y, -1)

	// substitute year {yy}
	s = strings.Replace(s, "{yy}", y[2:], -1)

	// substitute month {mm}
	m := fmt.Sprintf("%02d", int(dateHour.Month()))
	s = strings.Replace(s, "{mm}", m, -1)

	// substitute day {dd}
	d := fmt.Sprintf("%02d", dateHour.Day())
	s = strings.Replace(s, "{dd}", d, -1)

	// substitute hour {hh}
	h := fmt.Sprintf("%02d", dateHour.Hour())
	s = strings.Replace(s, "{hh}", h, -1)

	return s
}
