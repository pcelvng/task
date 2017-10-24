package main

import (
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

	Topic       string // topic override (uses 'TaskType' if not provided)
	DoneTopic   string // topic to return a done task
	TaskType    string
	FailRate    int
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
