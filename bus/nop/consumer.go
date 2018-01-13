package nop

import "errors"

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

	return &Consumer{mock}, nil
}

// Consumer is a no-operation consumer. It
// does not do anything and is useful for mocking.
type Consumer struct {
	// Mock can be set in order to
	// mock various return scenarios.
	//
	// Supported Values:
	// - "init_err" - returns err on NewConsumer
	// - "err" - every method returns an error
	// - "msg_err" - returns err on Consumer.Msg() call.
	// - "msg_done" - returns a nil task message done=true on Consumer.Msg() call.
	// - "msg_msg_done" - returns a non-nil task message and done=true Consumer.Msg() call.
	// - "stop_err" - returns err on Stop() method call
	Mock string
}

// Msg will always return a fake task message unless err != nil
// or Mock == "msg_done".
func (c *Consumer) Msg() (msg []byte, done bool, err error) {
	if c.Mock == "msg_err" {
		return msg, done, errors.New(c.Mock)
	}

	if c.Mock == "msg_done" {
		return msg, true, err
	}

	if c.Mock == "msg_msg_done" {
		done = true
	}

	// set fake msg
	msg = FakeMsg
	return msg, done, err
}

// Stop is a mock consumer Stop method.
func (c *Consumer) Stop() error {
	if c.Mock == "stop_err" {
		return errors.New(c.Mock)
	}
	return nil
}
