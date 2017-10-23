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

	// retry rules
	RetryRules []*RetryRule `toml:"rule"`
}

type RetryRule struct {
	TaskType    string `toml:"task_type"`
	Retries     int    `toml:"retries"`
	WaitMinutes int    `toml:"wait_minutes"`
	Topic       string `toml:"topic"` // topic override
}

func LoadConfig(filePath string) (*Config, error) {
	c := NewConfig()

	if _, err := toml.DecodeFile(filePath, c); err != nil {
		return nil, err
	}

	return c, nil
}
