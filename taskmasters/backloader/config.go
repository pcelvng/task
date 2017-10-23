package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pcelvng/task/util"
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

func (c *Config) StartEndStrings(start, end string) error {
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
		return errors.New("'start' date required")
	}

	// End required
	if c.End.IsZero() {
		return errors.New("'end' date required")
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
