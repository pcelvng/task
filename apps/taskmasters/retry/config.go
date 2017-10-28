package main

import (
	"github.com/BurntSushi/toml"

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

	// topic and channel to listen to
	// done tasks for retry review.
	DoneTopic   string `toml:"done_topic"`
	DoneChannel string `toml:"done_channel"`

	// retry rules
	RetryRules []*RetryRule `toml:"rule"`
}

type RetryRule struct {
	TaskType string   `toml:"task_type"`
	Retries  int      `toml:"retries"`
	Wait     duration `toml:"wait"`
	Topic    string   `toml:"topic"` // topic override
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
