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

// ProducersConfig is a general config struct that
// can provides all potential config values for all
// bus producers. It can be used to dynamically
// create a producer using
type ProducersConfig struct {
	BusType string `toml:"task_bus"`

	// for "file" bus type
	FilePath string `toml:"file_path"`

	// for "nsq" bus type
	NsqdHosts []string `toml:"nsqd_hosts"`
}

// NewProducer will create a producer dynamically according to
// the value of conf.BusType.
//
// BusType supported values:
// - "stdout" (default if TaskBus is empty)
// - "file"
// - "nsq"
//
// For 'nsq' bus:
// - if no NsqdHosts are provided then it will default to 'localhost:4150'
func NewProducer(conf *ProducersConfig) (bus.Producer, error) {
	// setup bus producer
	var p bus.Producer
	var err error

	switch conf.BusType {
	case "stdout", "":
		p = iobus.NewStdoutProducer()
		break
	case "file":
		p, err = iobus.NewFileProducer(conf.FilePath)
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
			conf.BusType,
		))
	}

	return p, nil
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
