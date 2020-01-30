package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ps "cloud.google.com/go/pubsub"

	"github.com/pcelvng/task/bus/info"
)

type Producer struct {
	client *ps.Client

	// context for clean shutdown
	ctx  context.Context
	cncl context.CancelFunc

	info info.Producer

	mux sync.Mutex
}

// NewProducer will create a new pubsub producer for publishing messages to pubsub
func (o *Option) NewProducer() (p *Producer, err error) {
	p = &Producer{
		info: info.Producer{Bus: "pubsub", Sent: make(map[string]int)},
	}

	// create context for clean shutdown
	p.ctx, p.cncl = context.WithCancel(context.Background())

	p.client, err = o.newClient()
	if err != nil {
		return nil, err
	}

	return p, nil
}

// Send will send one message to the topic
// Settings are for publishing one message at a time with a 100 millisecond delay
// locking is used to update the topic count of published messages.
func (p *Producer) Send(topic string, msg []byte) (err error) {
	// should not attempt to send if producer already stopped.
	if p.ctx.Err() != nil {
		errMsg := fmt.Sprintf("unable to send '%v'; producer already stopped", string(msg))
		return errors.New(errMsg)
	}

	t := p.client.Topic(topic)
	t.PublishSettings.CountThreshold = 1
	t.PublishSettings.DelayThreshold = 100 * time.Millisecond

	defer t.Stop()

	ok, err := t.Exists(p.ctx)
	if err != nil {
		return err
	}
	if !ok {
		t, err = p.client.CreateTopic(p.ctx, topic)
		if err != nil {
			return err
		}
	}

	// publish message to pubsub
	res := t.Publish(p.ctx, &ps.Message{Data: msg})
	_, err = res.Get(p.ctx)
	if err != nil {
		return err
	}

	p.mux.Lock()
	p.info.Sent[topic]++
	p.mux.Unlock()

	return nil
}

func (p *Producer) Stop() (err error) {
	if p.ctx.Err() != nil {
		return nil
	}
	p.cncl()
	p.client.Close()

	return err
}

func (p *Producer) Info() (i info.Producer) {
	return i
}
