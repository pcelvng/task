package nop

import (
	"testing"

	"github.com/jbsmith7741/trial"
)

func TestNewConsumer(t *testing.T) {
	// doesn't error under normal behavior
	c, err := NewConsumer("")
	if c == nil || err != nil {
		t.Error("FAIL: default nop should not error")
	}

	// keyword:init_err should return error
	_, err = NewConsumer(Init_err)
	if err == nil {
		t.Error("FAIL: init_err should return error")
	}

}

func TestConsumer_Msg(t *testing.T) {
	type Output struct {
		msg  []byte
		done bool
	}
	fn := func(args ...interface{}) (interface{}, error) {
		mock := args[0].(string)
		c, err := NewConsumer(mock)
		if err != nil {
			return Output{}, err
		}
		msg, done, err := c.Msg()
		return Output{msg, done}, err
	}

	trial.New(fn, trial.Cases{
		"default": {
			Input:    "",
			Expected: Output{msg: FakeMsg},
		},
		"keyword: msg_err": {
			Input:     Msg_err,
			ShouldErr: true,
		},
		"keyword: msg_done": {
			Input: Msg_done,
			Expected: Output{
				msg:  nil,
				done: true,
			},
		},
		"keyword: msg_msg_done": {
			Input: Msg_msg_done,
			Expected: Output{
				msg:  FakeMsg,
				done: true,
			},
		},
	}).Test(t)
}

func TestConsumer_Info(t *testing.T) {
	c, _ := NewConsumer("")
	for i := 0; i < 10; i++ {
		c.Msg()
	}
	if c.Info().Received != 10 {
		t.Errorf("FAIL: messages recieved 10 != %d", c.Info().Received)
	}
}

func TestConsumer_Stop(t *testing.T) {
	// doesn't error under normal behavior
	c, _ := NewConsumer("")
	if err := c.Stop(); err != nil {
		t.Errorf("FAIL: default should not error %s", err)
	}

	// keyword:stop_err should return error
	c, _ = NewConsumer(Stop_err)
	if err := c.Stop(); err == nil {
		t.Error("FAIL: keyword:stop_err should error")
	}
}
