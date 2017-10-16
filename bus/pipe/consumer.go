package pipebus

import (
	"bufio"
	"os"
	"strings"

	"github.com/pcelvng/task"
)

func NewPipeConsumer() (*PipeConsumer, error) {
	return &PipeConsumer{}, nil
}

type PipeConsumer struct {
	stdIn *bufio.Scanner
}

func (c *PipeConsumer) Connect() error {
	c.stdIn = bufio.NewScanner(os.Stdin)

	return nil
}

func (c *PipeConsumer) Msg() ([]byte, error) {
	if c.stdIn.Scan() {
		return c.stdIn.Bytes(), nil
	} else {
		b := c.stdIn.Bytes()

		// check that the last line has bytes and is probably json.
		if len(b) > 0 {

		}
	}


	return nil, nil
}

func (c *PipeConsumer) Close() error {

	return nil
}
