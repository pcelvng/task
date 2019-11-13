package pubsub

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	ps "cloud.google.com/go/pubsub"
	"github.com/pcelvng/task/bus/info"
	"google.golang.org/api/option"
)

type Producer struct {
	client *ps.Client

	// context for clean shutdown
	ctx  context.Context
	cncl context.CancelFunc

	info info.Producer

	mux sync.Mutex
}

func (o *Option) NewProducer() (p *Producer, err error) {
	if o.Host != "" && o.Host != "/" {
		os.Setenv("PUBSUB_EMULATOR_HOST", o.Host)
	}

	if o.ProjectID != "" {
		os.Setenv("PUBSUB_PROJECT_ID", o.ProjectID)
	}

	opts := make([]option.ClientOption, 0)
	if o.Connections > 1 {
		opts = append(opts, option.WithGRPCConnectionPool(o.Connections))
	}

	if o.JSONAuth != "" {
		opts = append(opts, option.WithCredentialsFile(o.JSONAuth))
	}

	p = &Producer{
		info: info.Producer{Bus: "pubsub", Sent: make(map[string]int)},
	}

	// create context for clean shutdown
	p.ctx, p.cncl = context.WithCancel(context.Background())

	p.client, err = ps.NewClient(p.ctx, o.ProjectID, opts...)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// Send will send one message to the topic
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
	serverid, err := res.Get(p.ctx)
	if err != nil {
		return err
	}

	p.mux.Lock()
	p.info.Sent[topic]++
	p.mux.Unlock()

	fmt.Println("serverid", serverid)
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
