package io

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync"
)

func NewStdinConsumer() *Consumer {
	return &Consumer{
		readCloser: os.Stdin,
	}
}

func NewFileConsumer(path string) (*Consumer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		readCloser: f,
	}, nil
}

type Consumer struct {
	sync.Mutex
	readCloser io.ReadCloser
	scanner    *bufio.Scanner
}

func (c *Consumer) Connect() error {
	if c.scanner == nil {
		c.scanner = bufio.NewScanner(c.readCloser)
	}

	return nil
}

func (c *Consumer) Msg() (msg []byte, done bool, err error) {
	// Only one read from the file at a time
	c.Lock()
	defer c.Unlock()

	if c.scanner == nil {
		return msg, done, errors.New("consumer has not connected")
	}

	if c.scanner.Scan() {
		msg = c.scanner.Bytes()
		return msg, false, err
	} else if err = c.scanner.Err(); err != nil {
		done = true
		return nil, done, err
	}

	done = true
	return msg, done, err
}

func (c *Consumer) Close() error {
	if err := c.readCloser.Close(); err != nil {
		return err
	}

	return nil
}
