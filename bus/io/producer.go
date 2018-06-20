package io

import (
	"errors"
	"os"
	"sync"

	"github.com/pcelvng/task/bus/info"
)

func NewStdoutProducer() *Producer {
	return &Producer{
		isStdout: true,
		info: info.Producer{
			Bus:  "stdout",
			Sent: make(map[string]int),
		},
	}
}

func NewNullProducer() *Producer {
	return &Producer{
		isNull: true,
		info: info.Producer{
			Bus:  "null",
			Sent: make(map[string]int),
		},
	}
}

func NewStdErrProducer() *Producer {
	return &Producer{
		isStderr: true,
		info: info.Producer{
			Bus:  "stderr",
			Sent: make(map[string]int),
		},
	}
}

func NewProducer() *Producer {
	return &Producer{
		info: info.Producer{Sent: make(map[string]int)},
	}
}

type Producer struct {
	isStdout bool
	isNull   bool
	isStderr bool
	mu       sync.Mutex
	info     info.Producer
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

	if p.isNull {
		topic = "/dev/null"
	}

	if p.isStderr {
		topic = "/dev/stderr"
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
	p.info.Sent[topic]++
	_, err = f.Write(msg)
	f.Close()
	return err
}

func (p *Producer) Info() info.Producer {
	return p.info
}

func (p *Producer) Stop() error {
	return nil
}
