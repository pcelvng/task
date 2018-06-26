package nop

import (
	"testing"

	"github.com/jbsmith7741/trial"
	"github.com/pcelvng/task/bus/info"
	"github.com/stretchr/testify/assert"
)

func TestNewProducer(t *testing.T) {
	// doesn't error under normal behavior
	c, err := NewProducer("")
	if c == nil || err != nil {
		t.Error("FAIL: default nop should not error")
	}

	// keyword:init_err should return error
	_, err = NewProducer(InitErr)
	if err == nil {
		t.Error("FAIL: init_err should return error")
	}

}

func TestProducer_Send(t *testing.T) {
	type input struct {
		mock     string
		messages map[string][]string
	}
	fn := func(args ...interface{}) (interface{}, error) {
		in := args[0].(input)
		p, err := NewProducer(in.mock)
		if err != nil {
			return nil, err
		}
		for topic, msgs := range in.messages {
			for _, msg := range msgs {
				err := p.Send(topic, []byte(msg))
				if err != nil {
					return nil, err
				}
			}
		}
		return p.Messages, nil
	}
	trial.New(fn, trial.Cases{
		"single topic": {
			Input: input{
				mock:     "",
				messages: map[string][]string{"": {"hello"}},
			},
			Expected: map[string][]string{"": {"hello"}},
		},
		"multiple topics": {
			Input: input{
				mock:     "",
				messages: map[string][]string{"a": {"hello"}, "b": {"world"}},
			},
			Expected: map[string][]string{"a": {"hello"}, "b": {"world"}},
		},
		"keyword: send_err": {
			Input: input{
				mock:     Send_err,
				messages: map[string][]string{"": {"hello"}},
			},
			ShouldErr: true,
		},
	}).Test(t)
}

func TestProducer_Info(t *testing.T) {
	p, _ := NewProducer("")
	for i := 0; i < 5; i++ {
		p.Send("a", []byte{byte(i)})
	}
	for i := 0; i < 10; i++ {
		p.Send("b", []byte{byte(i)})
	}
	assert.Equal(t, info.Producer{
		Bus:  "mock",
		Sent: map[string]int{"a": 5, "b": 10},
	}, p.Info())
}

func TestProducer_Stop(t *testing.T) {
	// doesn't error under normal behavior
	c, _ := NewProducer("")
	if err := c.Stop(); err != nil {
		t.Errorf("FAIL: default should not error %s", err)
	}

	// keyword:stop_err should return error
	c, _ = NewProducer(StopErr)
	if err := c.Stop(); err == nil {
		t.Error("FAIL: keyword:stop_err should error")
	}
}
