package io

import (
	"bufio"
	"context"
	"io"
	"testing"

	"github.com/jbsmith7741/trial"
)

func TestNewConsumer(t *testing.T) {
	fn := func(args ...interface{}) (interface{}, error) {
		pth := args[0].(string)
		_, err := NewConsumer(pth)
		return nil, err
	}
	trial.New(fn, trial.Cases{
		"missing path": {
			Input:     "",
			ShouldErr: true,
		},
		"file does not exist": {
			Input:     "err.txt",
			ShouldErr: true,
		},
	}).Test(t)
}

type mockReader struct {
	lineCount int
	line      int
}

func (t *mockReader) Read(p []byte) (n int, err error) {
	if t.line >= t.lineCount-1 {
		return 0, io.EOF
	}
	t.line++
	p[0] = '\n'
	return 1, nil
}

func TestConsumer_Info(t *testing.T) {
	c := &Consumer{
		pth:     "test",
		scanner: bufio.NewScanner(&mockReader{lineCount: 10}),
	}
	c.ctx, c.cncl = context.WithCancel(context.Background())
	for done := false; !done; _, done, _ = c.Msg() {
	}

	if c.Info().Received != 10 {
		t.Errorf("Message received mismatch 10 != %d", c.Info().Received)
	}

}
