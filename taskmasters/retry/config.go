package main

import (
	"github.com/BurntSushi/toml"

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
	DoneTopic   string
	DoneChannel string

	// retry rules
	RetryRules []*RetryRule `toml:"rule"`
}

type RetryRule struct {
	TaskType    string `toml:"task_type"`
	Retries     int    `toml:"retries"`
	WaitMinutes int    `toml:"wait_minutes"`
	Topic       string `toml:"done_topic"` // topic override
}

func LoadConfig(filePath string) (*Config, error) {
	c := NewConfig()

	if _, err := toml.DecodeFile(filePath, c); err != nil {
		return nil, err
	}

	return c, nil
}
