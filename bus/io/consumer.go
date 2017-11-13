package io

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"sync"
)

func NewStdinConsumer() *Consumer {
	// context for safe shutdown
	ctx, cncl := context.WithCancel(context.Background())

	c := &Consumer{
		readCloser: os.Stdin,
		ctx:        ctx,
		cncl:       cncl,
	}

	c.connect()
	return c
}

func NewFileConsumer(path string) (*Consumer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// context for safe shutdown
	ctx, cncl := context.WithCancel(context.Background())

	c := &Consumer{
		readCloser: f,
		ctx:        ctx,
		cncl:       cncl,
	}

	err = c.connect()
	if err != nil {
		return nil, err
	}

	return c, nil
}

type Consumer struct {
	sync.Mutex
	readCloser io.ReadCloser
	scanner    *bufio.Scanner
	ctx        context.Context
	cncl       context.CancelFunc
}

func (c *Consumer) connect() error {
	if c.scanner == nil {
		c.scanner = bufio.NewScanner(c.readCloser)
	}

	return nil
}

func (c *Consumer) Msg() (msg []byte, done bool, err error) {
	// Read one msg at a time
	c.Lock()
	defer c.Unlock()
	if c.ctx.Err() != nil {
		// should not attempt to read if already stopped
		done = true
		return msg, done, nil
	}

	if c.scanner == nil {
		return msg, done, errors.New("consumer not connected")
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

func (c *Consumer) Stop() error {
	if c.ctx.Err() != nil {
		return nil
	}
	c.cncl()

	if err := c.readCloser.Close(); err != nil {
		return err
	}

	return nil
}
