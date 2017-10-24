package flex

import (
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/util"
)

func NewFlexReceiver(conf *util.BusesConfig, inTopic, outTopic string) (*FlexReceiver, error) {
	// make consumer
	c, err := util.NewConsumer(conf)
	if err != nil {
		return nil, err
	}

	// make producer
	p, err := util.NewProducer(conf)
	if err != nil {
		return nil, err
	}

	fr := &FlexReceiver{
		conf:     conf,
		consumer: c,
		producer: p,
		inTopic:  inTopic,
		outTopic: outTopic,
	}

	return fr, nil

}

// FlexReceiver can flexibly read from
// one bus type and write to a bus of a different
// type.
// For example, it could be configured to read tasks from
// stdin and write tasks to nsq.
type FlexReceiver struct {
	conf     *util.BusesConfig
	consumer bus.Consumer
	producer bus.Producer
	inTopic  string
	outTopic string
}

func (r *FlexReceiver) Connect() error {
	return nil
}

func (r *FlexReceiver) Next() (tsk *task.Task, done bool, err error) {
	return nil, false, nil
}

func (r *FlexReceiver) Done(*task.Task) {
	return
}

func (r *FlexReceiver) Close() error {
	return nil
}
