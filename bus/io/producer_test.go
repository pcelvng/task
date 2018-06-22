package io

import (
	"testing"

	"github.com/jbsmith7741/trial"
	"github.com/pcelvng/task/bus/info"
)

func TestProducer_Info(t *testing.T) {
	type input struct {
		producer *Producer
		messages map[string][]string
	}
	fn := func(args ...interface{}) (interface{}, error) {
		in := args[0].(input)
		for topic, msgs := range in.messages {
			for _, msg := range msgs {
				err := in.producer.Send(topic, []byte(msg))
				if err != nil {
					return nil, err
				}
			}
		}
		return in.producer.Info(), nil
	}
	trial.New(fn, trial.Cases{
		"null writer": {
			Input: input{
				producer: NewNullProducer(),
				messages: map[string][]string{"": {"hello", "world"}},
			},
			Expected: info.Producer{
				Bus:  "null",
				Sent: map[string]int{"/dev/null": 2},
			},
		},
		"stdout writer": {
			Input: input{
				producer: NewStdoutProducer(),
				messages: map[string][]string{"": {"quick brown fox"}},
			},
			Expected: info.Producer{
				Bus:  "stdout",
				Sent: map[string]int{"/dev/stdout": 1},
			},
		},
		"stderr writer": {
			Input: input{
				producer: NewStdErrProducer(),
				messages: map[string][]string{"": {"", "apple", "pinapple"}},
			},
			Expected: info.Producer{
				Bus:  "stderr",
				Sent: map[string]int{"/dev/stderr": 3},
			},
		},
		"topic required error": {
			Input: input{
				producer: NewProducer(),
				messages: map[string][]string{"": {"", "apple", "pinapple"}},
			},
			ShouldErr: true,
		},
	}).Test(t)
}
