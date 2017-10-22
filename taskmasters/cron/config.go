package main

import (
	"github.com/BurntSushi/toml"
)

type Config struct {
	TaskBus string `toml:"task_bus"`

	// "file" bus options
	FilePath string `toml:"file_path"`

	// "nsq" bus options
	NsqdHosts []string `toml:"nsqd_hosts"`

	// rules
	Rules []*Rule `toml:"rule"`
}

type Rule struct {
	CronRule   string `toml:"cron_rule"`
	TaskType   string `toml:"task_type"`
	TaskValue  string `toml:"task_value"`
	HourOffset int    `toml:"hour_offset"`
	Topic      string `toml:"topic"`
}

func LoadConfig(filePath string) (*Config, error) {
	c := &Config{}

	if _, err := toml.DecodeFile(filePath, c); err != nil {
		return nil, err
	}

	return c, nil
}
