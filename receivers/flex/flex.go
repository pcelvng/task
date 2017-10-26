package flex

import (
	"log"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus"
	"github.com/pcelvng/task/util"
)

func NewFlexReceiver(conf *util.BusesConfig, inTopic, inChannel, doneTopic string) (*FlexReceiver, error) {
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
		conf:      conf,
		consumer:  c,
		producer:  p,
		inTopic:   inTopic,
		inChannel: inChannel,
		doneTopic: doneTopic,
	}

	return fr, nil
}

// FlexReceiver can flexibly receive tasks from
// one bus type and write to a bus of a different
// type.
// For example, it could be configured to receive tasks from
// stdin and write tasks to nsq.
type FlexReceiver struct {
	conf      *util.BusesConfig
	consumer  bus.Consumer
	producer  bus.Producer
	inTopic   string
	inChannel string
	doneTopic string
}

func (r *FlexReceiver) Connect() error {
	if err := r.consumer.Connect(r.inTopic, r.inChannel); err != nil {
		return err
	}

	if err := r.producer.Connect(); err != nil {
		return err
	}

	return nil
}

func (r *FlexReceiver) Next() (tsk *task.Task, done bool, err error) {
	msg, done, err := r.consumer.Msg()
	if len(msg) == 0 {
		return tsk, done, err
	}

	tsk, err = task.NewFromBytes(msg)

	return tsk, done, err
}

func (r *FlexReceiver) Done(tsk *task.Task) {
	msg, err := tsk.Bytes()
	if err != nil {
		log.Println(err.Error())
	} else {
		r.producer.Send(r.doneTopic, msg)
	}

	return
}

func (r *FlexReceiver) Close() error {
	err := r.consumer.Close()
	err = r.producer.Close()

	return err
}
