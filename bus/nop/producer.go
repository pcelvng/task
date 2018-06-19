package nop

import (
	"errors"
	"strings"
	"sync"
)

// NewProducer returns a nop (no-operation) Producer.
// Will return *Producer == nil and err != nil
// if mock == "init_err".
func NewProducer(mock string) (*Producer, error) {
	if mock == "init_err" {
		return nil, errors.New(mock)
	}

	return &Producer{
		Mock:     mock,
		Messages: make(map[string][]string, 0),
	}, nil
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
	Mock     string
	Messages map[string][]string // [topic]Messages
	mu       sync.Mutex
}

func (p *Producer) Send(topic string, msg []byte) error {
	if p.Mock == "send_err" {
		return errors.New(p.Mock)
	}
	p.mu.Lock()
	p.Messages[topic] = append(p.Messages[topic], string(msg))
	p.mu.Unlock()
	return nil
}

// Stop is a mock producer Stop method.
func (p *Producer) Stop() error {
	if p.Mock == "stop_err" {
		return errors.New(p.Mock)
	}
	return nil
}

func (p *Producer) Contains(topic string, msg []byte) bool {
	for _, m := range p.Messages[topic] {
		if strings.Contains(m, string(msg)) {
			return true
		}
	}
	return false
}
