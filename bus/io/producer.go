package io

import (
	"errors"
	"os"
	"sync"
)

func NewStdoutProducer() *Producer {
	return &Producer{isStdout: true}
}

func NewProducer() *Producer {
	return &Producer{}
}

type Producer struct {
	isStdout bool
	mu       sync.Mutex
}

// Send takes a topic and the message to send. The topic is
// a file name and is required unless initialized with
// NewStdoutProducer.
//
// If initialized with NewStdoutProducer then the msg will
// always be written to "/dev/stdout".
func (p *Producer) Send(topic string, msg []byte) error {
	if p.isStdout {
		topic = "/dev/stdout" // topic is always stdout.
	}

	if topic == "" {
		return errors.New("topic required")
	}

	if (len(msg) == 0) || (msg[len(msg)-1] != '\n') {
		msg = append(msg, '\n')
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	f, err := os.OpenFile(topic, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	_, err = f.Write(msg)
	f.Close()
	return err
}

func (p *Producer) Stop() error {
	return nil
}
