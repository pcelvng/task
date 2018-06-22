package io

import (
	"bufio"
	"context"
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/pcelvng/task/bus/info"
)

func NewConsumer(pth string) (*Consumer, error) {
	if pth == "" {
		return nil, errors.New("path required")
	}

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
		info: info.Consumer{
			Bus: pth,
		},
	}

	return c, nil
}

// Consumer must be initialized with NewConsumer or calling Msg()
// will panic.
type Consumer struct {
	f       *os.File
	scanner *bufio.Scanner // scanners have line length limit, but should not be a problem here
	pth     string         // file path. equivalent to a 'topic' on other buses.

	ctx  context.Context
	cncl context.CancelFunc
	sync.Mutex
	info info.Consumer
}

func (c *Consumer) Msg() (msg []byte, done bool, err error) {
	if c.ctx.Err() != nil {
		return msg, true, nil
	}

	// don't allow concurrent calls to Scan()
	// otherwise scanner.Bytes() will
	// not be consistent.
	c.Lock()
	defer c.Unlock()

	scanChan := make(chan interface{})
	go func() {
		if c.scanner.Scan() {
			msg = c.scanner.Bytes()
		} else {
			done = true
		}

		close(scanChan)
	}()

	select {
	case <-scanChan:
		c.info.Received++
	case <-c.ctx.Done():
	}

	return msg, done, err
}

func (c *Consumer) Info() info.Consumer {
	return c.info
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
