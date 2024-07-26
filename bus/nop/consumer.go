package nop

import (
	"errors"
	"sync"

	"github.com/pcelvng/task/bus/info"
)

const (
	InitErr    = "init_err"     // returns err on NewConsumer
	MsgErr     = "msg_err"      // returns err on Consumer.Msg() call.
	MsgDone    = "msg_done"     // returns a nil task message done=true on Consumer.Msg() call.
	MsgMsgDone = "msg_msg_done" // returns a non-nil task message and done=true Consumer.Msg() call.
	Repeat     = "repeat"       // message will not be removed from queue and consumer with always return a message
	StopErr    = "stop_err"     // returns err on Stop() method call
)

// FakeMsg can be set to control the returned
// Msg() msg value.
const FakeMsg = `{"type":"test","info":"test-info","created":"2017-01-01T00:00:01Z"}`

// NewConsumer returns a nop (no-operation) Consumer.
// Will return *Consumer == nil and err != nil
// if mock == "init_err".
func NewConsumer(mock string, msgs ...string) (*Consumer, error) {
	if mock == "init_err" {
		return nil, errors.New(mock)
	}

	if len(msgs) == 0 {
		msgs = []string{FakeMsg}
	}

	return &Consumer{
		Mock:     mock,
		Stats:    info.Consumer{Bus: "nop"},
		messages: msgs,
	}, nil
}

// Consumer is a no-operation consumer. It
// does not do anything and is useful for mocking.
type Consumer struct {

	// Mock can be for mocking Consumer
	// see const above for supported values
	// Supported Values:
	// - "init_err" - returns err on NewConsumer
	// - "err" - every method returns an error
	// - "msg_err" - returns err on Consumer.Msg() call.
	// - "msg_done" - returns a nil task message done=true on Consumer.Msg() call.
	// - "msg_msg_done" - returns a non-nil task message and done=true Consumer.Msg() call.
	// - "stop_err" - returns err on Stop() method call
	Mock  string
	Stats info.Consumer
	mu    sync.Mutex

	// Message queue that will be returned when called.
	messages []string
	index    int
}

// Msg will always return a fake task message unless err != nil
// or Mock == "msg_done".
func (c *Consumer) Msg() (msg []byte, done bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Mock == MsgErr {
		return nil, false, errors.New(c.Mock)
	}

	if c.Mock == MsgDone || c.index >= len(c.messages) {
		return nil, true, nil
	}

	if c.Mock == MsgMsgDone {
		done = true
	}

	// set fake msg
	msg = []byte(c.messages[c.index])
	c.Stats.Received++
	c.index++
	if c.Mock == Repeat && c.index >= len(c.messages) {
		c.index = 0
	}

	return msg, done, err
}

func (c *Consumer) Info() info.Consumer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Stats
}

// Stop is a mock consumer Stop method.
func (c *Consumer) Stop() error {
	if c.Mock == "stop_err" {
		return errors.New(c.Mock)
	}
	return nil
}
