package main

import (
	"github.com/BurntSushi/toml"

	"github.com/pcelvng/task/bus"
)

func NewConfig() *Config {
	return &Config{
		BusConfig: bus.NewBusConfig(""),
	}
}

type Config struct {
	*bus.BusConfig

	// rules
	Rules []*Rule `toml:"rule"`
}

type Rule struct {
	CronRule     string `toml:"cron"`
	TaskType     string `toml:"type"` // also default topic
	TaskTemplate string `toml:"template"`
	HourOffset   int    `toml:"offset"`
	Topic        string `toml:"topic"` // topic override
}

func LoadConfig(filePath string) (*Config, error) {
	c := NewConfig()

	if _, err := toml.DecodeFile(filePath, c); err != nil {
		return nil, err
	}
	return c, nil
}
