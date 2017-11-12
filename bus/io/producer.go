package io

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

func NewStdoutProducer() *Producer {
	return &Producer{
		writeCloser: os.Stdout,
		writer:      bufio.NewWriter(os.Stdout),
	}
}

func NewFileProducer(path string) (*Producer, error) {
	f, err := os.Create(path) // will truncate file if already exists
	if err != nil {
		return nil, err
	}

	return &Producer{
		writeCloser: f,
		writer:      bufio.NewWriter(f),
	}, nil
}

type Producer struct {
	sync.Mutex
	writeCloser io.WriteCloser
	writer      *bufio.Writer
	stopped     bool
}

func (p *Producer) Connect() error {
	return nil
}

func (p *Producer) Send(_ string, msg []byte) error {
	if (len(msg) == 0) || (msg[len(msg)-1] != '\n') {
		msg = append(msg, '\n')
	}

	p.Lock()
	defer p.Unlock()

	// should not attempt to send if producer already stopped.
	if p.stopped {
		errMsg := fmt.Sprintf("unable to send '%v'; producer already stopped", string(msg))
		return errors.New(errMsg)
	}

	_, err := p.writer.Write(msg)
	if err != nil {
		return err
	}
	p.writer.Flush()

	return err
}

func (p *Producer) Stop() error {
	p.Lock()
	defer p.Unlock()

	if p.stopped {
		return nil
	}
	p.stopped = true
	if err := p.writeCloser.Close(); err != nil {
		return err
	}

	return nil
}
