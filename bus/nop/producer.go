package nop

import "errors"

// NewProducer returns a nop (no-operation) Producer.
// Will return *Producer == nil and err != nil
// if mock == "init_err".
func NewProducer(mock string) (*Producer, error) {
	if mock == "init_err" {
		return nil, errors.New(mock)
	}

	return &Producer{mock}, nil
}

// Producer is a no-operation consumer. It
// does not do anything and is useful for mocking.
type Producer struct {
	// Mock can be set in order to
	// mock various return scenarios.
	//
	// Supported Values:
	// - "init_err" - returns err on NewProducer
	// - "err" - every method returns an error
	// - "send_err" - returns err when Producer.Send() is called.
	// - "stop_err" - returns err on Stop() method call
	Mock string
}

func (p *Producer) Send(topic string, msg []byte) error {
	if p.Mock == "send_err" {
		return errors.New(p.Mock)
	}
	return nil
}

// Stop is a mock producer Stop method.
func (p *Producer) Stop() error {
	if p.Mock == "stop_err" {
		return errors.New(p.Mock)
	}
	return nil
}
