package main

import (
	"time"

	"github.com/BurntSushi/toml"

	"github.com/pcelvng/task/bus"
)

var (
	defaultDoneTopic   = "done"
	defaultDoneChannel = "retry"
)

func NewConfig() *Config {
	return &Config{
		BusConfig:   bus.NewBusConfig(""),
		DoneTopic:   defaultDoneTopic,
		DoneChannel: defaultDoneChannel,
	}
}

type Config struct {
	*bus.BusConfig

	// topic and channel to listen to
	// done tasks for retry review.
	DoneTopic   string `toml:"done_topic"`
	DoneChannel string `toml:"done_channel"`

	// retry rules
	RetryRules []*RetryRule `toml:"rule"`
}

type RetryRule struct {
	TaskType string   `toml:"type"`
	Retries  int      `toml:"retry"`
	Wait     duration `toml:"wait"`  // duration to wait before creating and sending new task
	Topic    string   `toml:"topic"` // topic override (default is TaskType value)
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error

	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func LoadConfig(filePath string) (*Config, error) {
	c := NewConfig()

	if _, err := toml.DecodeFile(filePath, c); err != nil {
		return nil, err
	}

	return c, nil
}
