package io

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

func NewStdoutProducer() *Producer {
	// create context for clean shutdown
	ctx, cncl := context.WithCancel(context.Background())

	return &Producer{
		writeCloser: os.Stdout,
		writer:      bufio.NewWriter(os.Stdout),
		ctx:         ctx,
		cncl:        cncl,
	}
}

func NewFileProducer(path string) (*Producer, error) {
	f, err := os.Create(path) // will truncate file if already exists
	if err != nil {
		return nil, err
	}

	// create context for clean shutdown
	ctx, cncl := context.WithCancel(context.Background())

	return &Producer{
		writeCloser: f,
		writer:      bufio.NewWriter(f),
		ctx:         ctx,
		cncl:        cncl,
	}, nil
}

type Producer struct {
	sync.Mutex
	writeCloser io.WriteCloser
	writer      *bufio.Writer
	ctx         context.Context
	cncl        context.CancelFunc
}

func (p *Producer) Send(_ string, msg []byte) error {
	if (len(msg) == 0) || (msg[len(msg)-1] != '\n') {
		msg = append(msg, '\n')
	}

	p.Lock()
	defer p.Unlock()

	// should not attempt to send if already stopped.
	if p.ctx.Err() != nil {
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
	if p.ctx.Err() != nil {
		return nil
	}
	p.cncl()

	if err := p.writeCloser.Close(); err != nil {
		return err
	}

	return nil
}
