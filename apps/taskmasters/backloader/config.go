package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pcelvng/task/bus"
)

var (
	taskType    = flag.String("type", "", "REQUIRED; the task type")
	t           = flag.String("t", "", "alias of 'type'")
	at          = flag.String("at", "", "alias of 'from' flag")
	from        = flag.String("from", "now", "format 'yyyy-mm-ddThh' (example: '2017-01-03T01'). Allows a special keyword 'now'.")
	to          = flag.String("to", "", "same format as 'from'; if not specified, will run the one hour specified by from. Allows special keyword 'now'.")
	outBus      = flag.String("bus", "stdout", "one of 'stdout', 'file', 'nsq'")
	b           = flag.String("b", "", "alias of 'bus'")
	outFile     = flag.String("out-file", "./out.tasks.json", "file bus path and name when 'file' task-bus specified")
	nsqdHosts   = flag.String("nsqd-hosts", "localhost:4150", "comma-separated list of nsqd hosts with port")
	template    = flag.String("template", "{yyyy}-{mm}-{dd}T{hh}:00", "task template")
	topic       = flag.String("topic", "", "overrides task type as the default topic")
	everyXHours = flag.Uint("every-x-hours", 0, "will generate a task every x hours. Includes the first hour. Can be combined with 'on-hours' and 'off-hours' options.")
	onHours     = flag.String("on-hours", "", "comma separated list of hours to indicate which hours of a day to back-load during a 24 period (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. Example: '0,4,15' - will only generate tasks on hours 0, 4 and 15")
	offHours    = flag.String("off-hours", "", "comma separated list of hours to indicate which hours of a day to NOT create a task (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. If used will trump 'on-hours' values. Example: '2,9,16' - will generate tasks for all hours except 2, 9 and 16.")

	dFmt = "2006-01-02T15"
)

func NewConfig() *Config {
	return &Config{
		BusConfig: bus.NewBusConfig(""),
	}
}

type Config struct {
	*bus.BusConfig

	Start time.Time // start of backload
	End   time.Time // end of backload

	Topic        string // topic override (uses 'TaskType' if not provided)
	TaskType     string
	TaskTemplate string

	EveryXHours int    // default skips 0 hours aka does all hours. Will always at least create a task for the start date.
	OnHours     []bool // each key represents the hour and bool is if that value is turned on. (not specified means all hours are ON)
	OffHours    []bool // each key represents the hour and bool is if that value is turned off.
}

// NsqdHostsString will set Config.NsqdHosts from a comma
// separated string of hosts.
func (c *Config) NsqdHostsString(hosts string) {
	c.NsqdHosts = strings.Split(hosts, ",")
}

// SetOnHours will parse onHours string and set
// OnHours value.
func (c *Config) SetOnHours(onHours string) error {
	hrs, err := parseHours(onHours)
	if err != nil {
		return err
	}

	c.OnHours = hrs
	return nil
}

// SetOffHours will parse onHours string and set
// OnHours value.
func (c *Config) SetOffHours(offHours string) error {
	hrs, err := parseHours(offHours)
	if err != nil {
		return err
	}

	c.OffHours = hrs
	return nil
}

func parseHours(hrsStr string) (hrs []bool, err error) {
	// make hrs exactly 24 slots
	hrs = make([]bool, 24)

	if hrsStr == "" {
		return hrs, err
	}

	// basic sanitation - remove spaces
	hrsStr = strings.Replace(hrsStr, " ", "", -1)

	// convert, sort, de-duplicate
	for _, hour := range strings.Split(hrsStr, ",") {
		hr, err := strconv.Atoi(hour)
		if err != nil {
			return hrs, errors.New(
				fmt.Sprintf("invalid hour value '%v'", hour))
		}
		if 0 <= hr && hr <= 23 {
			hrs[hr] = true
		} else {
			return hrs, errors.New(
				fmt.Sprintf("invalid hour value '%v' must be int between 0 and 23", hour))
		}
	}

	return hrs, nil
}

func (c *Config) DateRangeStrings(start, end string) error {
	// parse start
	s, err := time.Parse(dFmt, start)
	if err != nil {
		log.Println("cannot parse start")
		return err
	}

	// truncate to hour and assign
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
	// TaskType is required
	if c.TaskType == "" {
		return errors.New("flag '-type' or '-t' required")
	}

	return nil
}

func LoadConfig() (*Config, error) {
	flag.Parse()

	// load config
	c := NewConfig()
	c.TaskType = *taskType
	c.TaskTemplate = *template
	c.EveryXHours = int(*everyXHours)
	c.Topic = *topic
	c.OutBus = *outBus
	c.OutFile = *outFile
	c.NsqdHostsString(*nsqdHosts)
	if err := c.SetOnHours(*onHours); err != nil {
		return nil, err
	}

	if err := c.SetOffHours(*offHours); err != nil {
		return nil, err
	}

	// alias overrides
	if *t != "" {
		c.TaskType = *t
	}

	if *b != "" {
		c.OutBus = *b
	}

	from := *from
	to := *to
	if *at != "" {
		from = *at
		to = *at
	}

	now := time.Now().Format(dFmt) // 2017-01-03T01
	if from == "now" {
		from = now
	}

	if to == "now" {
		to = now
	}

	if err := c.DateRangeStrings(from, to); err != nil {
		return nil, err
	}

	return c, nil
}
