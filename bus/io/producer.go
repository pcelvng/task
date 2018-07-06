package io

import (
	"errors"
	"os"
	"sync"

	"github.com/pcelvng/task/bus/info"
)

func NewStdoutProducer() *Producer {
	return &Producer{
		file: os.Stdout,
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
		file: os.Stderr,
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
	file   *os.File
	isNull bool
	mu     sync.Mutex
	info   info.Producer
}

// Send takes a topic and the message to send. The topic is
// a file name and is required unless initialized with
// NewStdoutProducer.
//
// If initialized with NewStdoutProducer then the msg will
// always be written to "/dev/stdout".
func (p *Producer) Send(topic string, msg []byte) (err error) {
	if p.isNull {
		topic = "/dev/null"
	}
	if topic == "" {
		return errors.New("topic required")
	}

	var f *os.File

	if p.file != nil {
		f = p.file
	} else {
		f, err = os.OpenFile(topic, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
	}

	if (len(msg) == 0) || (msg[len(msg)-1] != '\n') {
		msg = append(msg, '\n')
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.info.Sent[topic]++
	_, err = f.Write(msg)
	return err
}

func (p *Producer) Info() info.Producer {
	return p.info
}

func (p *Producer) Stop() error {
	return nil
}
