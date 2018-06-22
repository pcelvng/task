package nop

import (
	"errors"

	"github.com/pcelvng/task/bus/info"
)

const (
	// Init_err returns err on NewConsumer
	Init_err = "init_err"
	Msg_err  = "msg_err" // returns err on Consumer.Msg() call.
	//Err = "err" //  every method returns an error
	Msg_done     = "msg_done"     // returns a nil task message done=true on Consumer.Msg() call.
	Msg_msg_done = "msg_msg_done" // returns a non-nil task message and done=true Consumer.Msg() call.
	Stop_err     = "stop_err"     // returns err on Stop() method call
)

// FakeMsg can be set to control the returned
// Msg() msg value.
var FakeMsg = []byte(`{"type":"test","info":"test-info","created":"2017-01-01T00:00:01Z"}`)

// NewConsumer returns a nop (no-operation) Consumer.
// Will return *Consumer == nil and err != nil
// if mock == "init_err".
func NewConsumer(mock string) (*Consumer, error) {
	if mock == "init_err" {
		return nil, errors.New(mock)
	}

	return &Consumer{Mock: mock, info: info.Consumer{Bus: "nop"}}, nil
}

// Consumer is a no-operation consumer. It
// does not do anything and is useful for mocking.
type Consumer struct {

	// Mock can be for mocking Consumer
	// see const above for supported values
	Mock string
	info info.Consumer
}

// Msg will always return a fake task message unless err != nil
// or Mock == "msg_done".
func (c *Consumer) Msg() (msg []byte, done bool, err error) {
	if c.Mock == "msg_err" {
		return msg, false, errors.New(c.Mock)
	}

	if c.Mock == "msg_done" {
		return msg, true, err
	}

	if c.Mock == "msg_msg_done" {
		done = true
	}

	// set fake msg
	c.info.Received++
	msg = FakeMsg
	return msg, done, err
}

func (c *Consumer) Info() info.Consumer {
	return c.info
}

// Stop is a mock consumer Stop method.
func (c *Consumer) Stop() error {
	if c.Mock == "stop_err" {
		return errors.New(c.Mock)
	}
	return nil
}
