package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"flag"

	"github.com/pcelvng/task/util"
)

var (
	taskType   = flag.String("type", "", "REQUIRED; the task type")
	t          = flag.String("t", "", "alias of 'type'")
	at         = flag.String("at", "", "alias of 'from' flag")
	from       = flag.String("from", "", "REQUIRED; format 'yyyy-mm-ddThh' (example: '2017-01-03T01')")
	to         = flag.String("to", "", "same format as 'from'; if not specified, will run the one hour specified by from")
	taskBus    = flag.String("bus", "stdout", "one of 'stdout', 'file', 'nsq'")
	b          = flag.String("b", "", "alias of 'bus'")
	outFile    = flag.String("out-file", "./out.tasks.json", "file bus path and name when 'file' task-bus specified")
	nsqdHosts  = flag.String("nsqd-hosts", "localhost:4150", "comma-separated list of nsqd hosts with port")
	template   = flag.String("template", "{yyyy}-{mm}-{dd}T{hh}:00", "task template")
	topic      = flag.String("topic", "", "overrides task type as the default topic")
	skipXHours = flag.Uint("skip-x-hours", 0, "will generate tasks skipping x hours")
	onHours    = flag.String("on-hours", "", "comma separated list of hours to indicate which hours of a day to backload during a 24 period (each value must be between 0-23). Example '0,4,15' - will only generate tasks on hours 0, 4 and 15")
)

func NewConfig() *Config {
	return &Config{
		BusesConfig: &util.BusesConfig{},
	}
}

type Config struct {
	*util.BusesConfig

	Start time.Time // start of backload
	End   time.Time // end of backload

	Topic        string // topic override (uses 'TaskType' if not provided)
	TaskType     string
	TaskTemplate string

	// TODO: implement SkipXHours and OnHours
	SkipXHours int
	OnHours    []int // each value from 0-23
}

// NsqdHostsString will set Config.NsqdHosts from a comma
// separated string of hosts.
func (c *Config) NsqdHostsString(hosts string) {
	c.NsqdHosts = strings.Split(hosts, ",")
}

// OnEveryString will attempt to convert a comma-separated
// string of int values. Will return error if one or values
// does not convert to to int.
func (c *Config) OnHoursString(onHours string) error {
	if onHours == "" {
		return nil
	}

	// basic sanitation - remove spaces
	onHours = strings.Replace(onHours, " ", "", -1)
	hoursStr := strings.Split(onHours, ",")

	var hours []int
	for _, hour := range hoursStr {
		hourInt, err := strconv.Atoi(hour)
		if err != nil {
			return errors.New(
				fmt.Sprintf("invalid hour value '%v' in '-on-hour' flag", hour))
		}

		hours = append(hours, hourInt)
	}

	c.OnHours = hours

	return nil
}

func (c *Config) DateRangeStrings(start, end string) error {
	dFmt := "2006-01-02T15"
	// parse start
	s, err := time.Parse(dFmt, start)
	if err != nil {
		log.Println("cannot parse start")
		return err
	}

	// round to hour and assign
	c.Start = s.Truncate(time.Hour)

	// start and end are equal if end not provided
	if end == "" {
		c.End = c.Start
		return nil
	}

	// parse end (if provided)
	e, err := time.Parse(dFmt, end)
	if err != nil {
		return err
	}

	// round to hour and assign
	c.End = e.Truncate(time.Hour)

	return nil
}

func (c *Config) Validate() error {
	// Start required
	if c.Start.IsZero() {
		return errors.New("'from' date required")
	}

	// End required
	if c.End.IsZero() {
		return errors.New("'to' date required")
	}

	// Start before End
	diff := int(c.End.Sub(c.Start))
	if diff < 0 {
		return errors.New("'start' must occur before 'end' date")
	}

	// TaskType is required
	if c.TaskType == "" {
		return errors.New("flag 'task-type' required")
	}

	// SkipEvery must be a positive integer
	if c.SkipXHours < 0 {
		return errors.New("'-skip-x-hours' must be positive")
	}

	// OnHours values must be positive value from 0-23
	for _, hour := range c.OnHours {
		if hour < 0 || hour > 23 {
			return errors.New("flag '-on-hour' values must be integers from 0-23")
		}
	}

	// cannot specify both 'SkipXHours' and 'OnHours'
	if c.SkipXHours > 0 && len(c.OnHours) > 0 {
		return errors.New("cannot specify both 'skip-x-hours' and 'on-hours' flags")
	}

	return nil
}

func LoadConfig() (*Config, error) {
	flag.Parse()

	// load config
	c := NewConfig()
	c.TaskType = *taskType
	c.TaskTemplate = *template
	c.SkipXHours = int(*skipXHours)
	c.Topic = *topic
	c.OutBus = *taskBus
	c.OutFile = *outFile
	c.NsqdHostsString(*nsqdHosts)
	if err := c.OnHoursString(*onHours); err != nil {
		return nil, err
	}

	// alias overrides
	if *t != "" {
		c.TaskType = *t
	}

	if *b != "" {
		c.Bus = *b
	}

	from := *from
	to := *to
	if *at != "" {
		from = *at
		to = *at
	}
	if err := c.DateRangeStrings(from, to); err != nil {
		return nil, err
	}

	return c, nil
}
