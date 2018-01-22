package io

import (
	"bufio"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
)

func NewConsumer(pth string) (*Consumer, error) {
	var f *os.File
	var err error

	// special handling of device files
	switch pth {
	case "/dev/stdin":
		f = os.Stdin
	default:
		f, err = os.Open(pth)
		if err != nil {
			return nil, err
		}
	}

	// context for safe shutdown
	ctx, cncl := context.WithCancel(context.Background())

	c := &Consumer{
		scanner: bufio.NewScanner(f),
		f:       f,
		pth:     pth,
		ctx:     ctx,
		cncl:    cncl,
	}

	return c, nil
}

type Consumer struct {
	f       *os.File
	scanner *bufio.Scanner // scanners have line length limit, but should not be a problem here
	pth     string

	ctx  context.Context
	cncl context.CancelFunc
	sync.Mutex
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

	// don't close device files
	if strings.HasPrefix(c.pth, "/dev/") {
		return nil
	}

	return c.f.Close()
}
