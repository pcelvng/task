package pubsub

import (
	"context"
	"errors"
	"log"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/pcelvng/task/bus/info"
)

type Consumer struct {
	// Client is a Google Pub/Sub client scoped to a single project.
	// Clients should be reused rather than being created as needed.
	// A Client may be shared by multiple goroutines. Consumer Client
	client *pubsub.Client
	sub    *pubsub.Subscription

	// context for clean shutdown
	ctx     context.Context
	cncl    context.CancelFunc
	msgChan chan *pubsub.Message
	info    info.Consumer
}

// NewConsumer creates a new consumer for reading messages from pubsub
func (o *Option) NewConsumer() (c *Consumer, err error) {
	c = &Consumer{
		info: info.Consumer{
			Bus:   "pubsub",
			Topic: o.Topic,
		},
		msgChan: make(chan *pubsub.Message),
	}

	c.client, err = o.newClient()
	if err != nil {
		return nil, err
	}

	// check for the topic if it doesn't exist create it to use for the subscription
	// create context for clean shutdown
	c.ctx, c.cncl = context.WithCancel(context.Background())
	ctx, _ := context.WithTimeout(c.ctx, 5*time.Second)

	topic := c.client.Topic(o.Topic)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		topic, err = c.client.CreateTopic(ctx, o.Topic)
		if err != nil {
			return nil, err
		}
	}

	// get the subscription from the provided subscription name (id)
	c.sub = c.client.Subscription(o.Subscription)

	// if the subscription does not exist, create the subscription
	if ok, err := c.sub.Exists(ctx); !ok || err != nil {
		c.sub, err = c.client.CreateSubscription(ctx, o.Subscription, pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: 10 * time.Second,
		})
		if err != nil {
			return nil, err
		}
	}

	c.sub.ReceiveSettings.MaxOutstandingMessages = 1
	c.sub.ReceiveSettings.Synchronous = true

	go func() {
		fn := func(ctx context.Context, m *pubsub.Message) { c.msgChan <- m }
		for ; ; time.Sleep(10 * time.Second) {
			err := c.sub.Receive(c.ctx, fn)
			if err != nil {
				log.Println(err)
				if err != context.Canceled {
					return
				}
			}
		}
	}()

	return c, nil
}

// Msg never blocks
// sets subscription to only have, at max, one message in memory at a time
// sets subscription to synchronous to only allow one message in memory at a time
func (c *Consumer) Msg() (msg []byte, done bool, err error) {
	if c.ctx.Err() != nil {
		// should not attempt to read if already stopped
		return msg, true, nil
	}
	select {
	case m := <-c.msgChan:
		if m == nil {
			return msg, false, errors.New("nil pubsub message")
		}
		m.Ack()
		msg = m.Data
	case <-c.ctx.Done():
	}
	c.info.Received++

	return msg, done, err
}

// Once Stop has been called subsequent calls to Msg
// should not block and immediately return with
// msgChan == nil (or len == 0), done == true and err == nil.
func (c *Consumer) Stop() (err error) {
	if c.ctx.Err() != nil {
		return nil
	}

	c.cncl()
	c.client.Close()
	return nil
}

func (c *Consumer) Info() (i info.Consumer) {
	return c.info
}
