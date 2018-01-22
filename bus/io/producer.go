package io

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

func NewProducer(pth string) (*Producer, error) {
	var f *os.File
	var err error

	// special handling of device files
	switch pth {
	case "/dev/stdout":
		f = os.Stdout
	case "/dev/stderr":
		f = os.Stderr
	case "/dev/null":
		fallthrough
	default:
		f, err = os.Create(pth) // will truncate file if already exists
		if err != nil {
			return nil, err
		}
	}

	// create context for clean shutdown
	ctx, cncl := context.WithCancel(context.Background())

	return &Producer{
		pth:  pth,
		f:    f,
		ctx:  ctx,
		cncl: cncl,
	}, nil
}

type Producer struct {
	f   *os.File
	pth string // file path

	ctx  context.Context
	cncl context.CancelFunc
	mu   sync.Mutex
}

func (p *Producer) Send(_ string, msg []byte) error {
	if (len(msg) == 0) || (msg[len(msg)-1] != '\n') {
		msg = append(msg, '\n')
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// should not attempt to send if already stopped.
	if p.ctx.Err() != nil {
		errMsg := fmt.Sprintf("unable to send '%v'; producer already stopped", string(msg))
		return errors.New(errMsg)
	}

	_, err := p.f.Write(msg)
	if err != nil {
		return err
	}

	return err
}

func (p *Producer) Stop() error {
	if p.ctx.Err() != nil {
		return nil
	}
	p.cncl()

	// don't close device files
	if strings.HasPrefix(p.pth, "/dev/") {
		return nil
	}

	return p.f.Close()
}
