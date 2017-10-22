package io

import (
	"bufio"
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
}

func (c *Producer) Connect() error {
	return nil
}

func (c *Producer) Send(_ string, msg []byte) error {
	if (len(msg) == 0) || (msg[len(msg)-1] != '\n') {
		msg = append(msg, '\n')
	}

	c.Lock()
	defer c.Unlock()
	_, err := c.writer.Write(msg)
	if err != nil {
		return err
	}
	c.writer.Flush()

	return err
}

func (c *Producer) Close() error {
	if err := c.writeCloser.Close(); err != nil {
		return err
	}

	return nil
}
