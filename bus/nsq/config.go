package nsqbus

import (
	"log"

	nsq "github.com/bitly/go-nsq"
)

// Config is used for instantiating an NSQ Consumer or
// Producer. The Producer will ignore the Topic value.
type Config struct {
	Topic        string
	Channel      string
	NSQdAddrs    []string // connects via TCP only
	LookupdAddrs []string // connects via HTTP only

	// if nil then the default nsq logger is used
	Logger *log.Logger

	// default is nsq.LogLevelInfo. Only set if a
	// custom logger is provided.
	LogLvl nsq.LogLevel
}
