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

	// rules
	Rules []*Rule `toml:"rule"`
}

type Rule struct {
	CronRule     string `toml:"cron_rule"`
	TaskType     string `toml:"task_type"`
	TaskTemplate string `toml:"task_template"`
	HourOffset   int    `toml:"hour_offset"`
	Topic        string `toml:"topic"`
}

func LoadConfig(filePath string) (*Config, error) {
	c := NewConfig()

	if _, err := toml.DecodeFile(filePath, c); err != nil {
		return nil, err
	}

	return c, nil
}
