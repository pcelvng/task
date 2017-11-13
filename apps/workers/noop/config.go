package main

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/pcelvng/task/bus"
)

var (
	tskType      = flag.String("type", "", "REQUIRED the task type; default topic")
	msgBus       = flag.String("bus", "stdio", "'stdio', 'file', 'nsq'")
	inBus        = flag.String("in-bus", "", "one of 'stdin', 'file', 'nsq'; useful if you want the in and out bus to be different types.")
	outBus       = flag.String("out-bus", "", "one of 'stdout', 'file', 'nsq'; useful if you want the in and out bus to be different types.")
	inFile       = flag.String("in-file", "./in.tsks.json", "file bus path and name when 'file' task-bus specified")
	outFile      = flag.String("out-file", "./out.tsks.json", "file bus path and name when 'file' task-bus specified")
	nsqdHosts    = flag.String("nsqd-hosts", "localhost:4150", "comma-separated list of nsqd hosts with tcp port")
	lookupdHosts = flag.String("lookupd-hosts", "localhost:4161", "comma-separated list of lookupd hosts with http port")
	topic        = flag.String("topic", "", "override task type as topic")
	channel      = flag.String("channel", "", "override task type as channel")
	doneTopic    = flag.String("done-topic", "done", "topic to return the task after completion")
	failRate     = flag.Int("fail-rate", 0, "choose 0-100; the rate at which tasks will be marked with an error; does not support fractions of a percentage.")
	dur          = flag.String("duration", "1s", "'1s' = 1 second, '1m' = 1 minute, '1h' = 1 hour")
	durVariance  = flag.String("variance", "", "+ evenly distributed variation when a task completes; 1s = 1 second, 1m = 1 minute, 1h = 1 hour")
	workers      = flag.Int("workers", 1, "maximum number of workers running at one time; workers cannot be less than 1.")
)

func NewConfig() *Config {
	return &Config{
		BusConfig: bus.NewBusConfig(),
	}
}

type Config struct {
	*bus.BusConfig

	TaskType    string        // will be used as the default topic and channel
	Topic       string        // topic override (uses 'TaskType' if not provided)
	Channel     string        // channel to listen for tasks of type TaskType
	DoneTopic   string        // topic to return a done task
	FailRate    int           // int between 0-100 representing a percent
	Dur         time.Duration // how long the task will take to finish successfully
	DurVariance time.Duration // random adjustment to the Dur value

}

// NsqdHostsString will set Config.NsqdHosts from a comma
// separated string of hosts.
func (c *Config) NsqdHostsString(hosts string) {
	c.NsqdHosts = strings.Split(hosts, ",")
}

// DurString will parse the 'dur' string and attempt to
// convert it to a duration using time.ParseDuration and assign
// that value to c.Dur.
func (c *Config) DurString(dur string) error {
	d, err := time.ParseDuration(dur)
	if err != nil {
		return err
	}
	c.Dur = d

	return nil
}

func (c *Config) Validate() error {
	// must have a task type
	if c.TaskType == "" {
		return errors.New("task type is required")
	}

	// must have a done topic value
	if c.DoneTopic == "" {
		return errors.New("done topic is required")
	}

	return nil
}

// DurVarianceString will parse the 'dur' string and attempt to
// convert it to a duration using time.ParseDuration and assign
// that value to c.DurVariance.
func (c *Config) DurVarianceString(dur string) error {
	d, err := time.ParseDuration(dur)
	if err != nil {
		return err
	}
	c.DurVariance = d

	return nil
}

func LoadConfig() *Config {
	flag.Parse()

	// load config
	c := NewConfig()
	c.Bus = *msgBus
	c.InBus = *inBus
	c.OutBus = *outBus
	c.InFile = *inFile
	c.OutFile = *outFile
	c.TaskType = *tskType
	c.Topic = *tskType // default topic
	if *topic != "" {
		c.Topic = *topic
	}
	c.Channel = *tskType // default channel
	if *channel != "" {
		c.Channel = *channel
	}
	c.DoneTopic = *doneTopic
	c.FailRate = *failRate
	c.NsqdHostsString(*nsqdHosts)
	c.DurString(*dur)
	c.DurVarianceString(*durVariance)

	return c
}
