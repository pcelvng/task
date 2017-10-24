package nsq

import (
	"log"

	gonsq "github.com/bitly/go-nsq"
)

// Config is used for instantiating an NSQ Consumer or
// Producer. The Producer will ignore the Topic value.
type Config struct {
	NSQdAddrs    []string // connects via TCP only
	LookupdAddrs []string // connects via HTTP only

	// if nil then the default nsq logger is used
	Logger *log.Logger

	// default is nsq.LogLevelInfo. Only set if a
	// custom logger is provided.
	LogLvl gonsq.LogLevel
}
