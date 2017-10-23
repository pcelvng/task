package util

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pcelvng/task/bus"
	iobus "github.com/pcelvng/task/bus/io"
	nsqbus "github.com/pcelvng/task/bus/nsq"
)

var (
	defaultWritePath = "./out_tasks.json"
	defaultReadPath  = "./in_tasks.json"
)

// BusesConfig is a general config struct that
// provides all potential config values for all
// bus consumers.
//
// It can be used to easily and dynamically create
// a producer or consumer.
type BusesConfig struct {
	// Valid producer values:
	// - "" (stdout)
	// - "stdout"
	// - "file"
	// - "nsq"
	//
	// Valid consumer values:
	// - "" (stdin)
	// - "stdin"
	// - "file"
	// - "nsq"
	InBusType  string `toml:"in_bus"`
	OutBusType string `toml:"out_bus"`

	// for "file" bus type
	WritePath string `toml:"write_file"` // for file producer
	ReadPath  string `toml:"read_file"`  // for file consumer

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

	switch conf.OutBusType {
	case "stdout", "":
		p = iobus.NewStdoutProducer()
		break
	case "file":
		writePath := conf.WritePath
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
			nsqConf.NSQdAddrs = []string{"localhost:4150"}
		} else {
			nsqConf.NSQdAddrs = conf.NsqdHosts
		}

		p = nsqbus.NewProducer(nsqConf)
		break
	default:
		return nil, errors.New(fmt.Sprintf(
			"task bus '%v' not supported - choices are 'stdout', 'file' or 'nsq'",
			conf.OutBusType,
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

	switch conf.InBusType {
	case "stdin", "":
		c = iobus.NewStdinConsumer()
		break
	case "file":
		readPath := conf.ReadPath
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
			nsqConf.NSQdAddrs = []string{"localhost:4150"}
		}

		c, err = nsqbus.NewLazyConsumer(nsqConf)
		break
	default:
		return nil, errors.New(fmt.Sprintf(
			"task bus '%v' not supported - choices are 'stdin', 'file' or 'nsq'",
			conf.InBusType,
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
