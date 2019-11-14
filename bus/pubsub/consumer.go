package pubsub

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pcelvng/task/bus/info"
	"google.golang.org/api/option"
)

type Consumer struct {
	// Client is a Google Pub/Sub client scoped to a single project.
	// Clients should be reused rather than being created as needed.
	// A Client may be shared by multiple goroutines. Consumer Client
	client *pubsub.Client
	sub    *pubsub.Subscription

	// context for clean shutdown
	ctx  context.Context
	cncl context.CancelFunc

	info info.Consumer
}

func (o *Option) NewConsumer() (c *Consumer, err error) {
	opts := make([]option.ClientOption, 0)

	if o.Host != "" && o.Host != "/" {
		fmt.Println("setting PUBSUB_EMULATOR_HOST as", o.Host)
		os.Setenv("PUBSUB_EMULATOR_HOST", o.Host)
	}

	if o.ProjectID != "" {
		fmt.Println("setting PUBSUB_PROJECT_ID as", o.ProjectID)
		os.Setenv("PUBSUB_PROJECT_ID", o.ProjectID)
	}

	if o.JSONAuth != "" {
		opts = append(opts, option.WithCredentialsFile(o.JSONAuth))
	}

	c = &Consumer{
		info: info.Consumer{
			Bus:   "pubsub",
			Topic: o.Topic,
		},
	}

	// create context for clean shutdown
	c.ctx, c.cncl = context.WithCancel(context.Background())
	c.client, err = pubsub.NewClient(c.ctx, o.ProjectID, opts...)
	if err != nil {
		return nil, err
	}

	// this loop will wait for the topic to be created (retry 10 times)
	// if it cannot be created after the retries it will fail with a not exists
	topic := c.client.Topic(o.Topic)
	for retry := 0; ; retry++ {
		ok, err := topic.Exists(c.ctx)
		if err != nil {
			return nil, err
		}

		if ok {
			break
		}

		fmt.Printf("topic %s does not exist... waiting...", o.Topic)
		time.Sleep(time.Second * 3)
		if retry > 10 {
			return nil, fmt.Errorf("topic %s does not exist", o.Topic)
		}
	}

	// get the subscription for the provided subscription name (id)
	c.sub = c.client.Subscription(o.SubscriptionID)

	// if the subscription does not exist, create the subscription
	if ok, err := c.sub.Exists(c.ctx); !ok || err != nil {
		c.sub, err = c.client.CreateSubscription(c.ctx, o.SubscriptionID, pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: 10 * time.Second,
		})
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Msg never blocks, not sure if this is how it's supposed to work (needs testing)
func (c *Consumer) Msg() (msg []byte, done bool, err error) {
	if c.ctx.Err() != nil {
		// should not attempt to read if already stopped
		return msg, true, nil
	}

	c.sub.ReceiveSettings.MaxOutstandingMessages = 1
	c.sub.ReceiveSettings.Synchronous = true
	cctx, cancel := context.WithCancel(c.ctx)
	err = c.sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
		msg = m.Data
		m.Ack()
		c.info.Received++
		cancel()
	})

	return msg, done, err
}

// Once Stop has been called subsequent calls to Msg
// should not block and immediately return with
// msg == nil (or len == 0), done == true and err == nil.
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
