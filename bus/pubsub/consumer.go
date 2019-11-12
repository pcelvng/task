package pubsub

import (
	"context"
	"fmt"
	"os"

	psb "cloud.google.com/go/pubsub/apiv1"
	"github.com/pcelvng/task/bus/info"
	ps "google.golang.org/genproto/googleapis/pubsub/v1"
)

type Consumer struct {
	// Client is a Google Pub/Sub client scoped to a single project.
	// Clients should be reused rather than being created as needed.
	// A Client may be shared by multiple goroutines. Consumer Client
	client  *psb.SubscriberClient
	pullReq *ps.PullRequest

	// context for clean shutdown
	ctx  context.Context
	cncl context.CancelFunc

	info info.Consumer
}

func (o *Option) NewConsumer() (c *Consumer, err error) {
	if o.Host != "" && o.Host != "/" {
		os.Setenv("PUBSUB_EMULATOR_HOST", o.Host)
	}

	if o.ProjectID != "" {
		os.Setenv("PUBSUB_PROJECT_ID", o.ProjectID)
	}

	c = &Consumer{
		info: info.Consumer{
			Bus:   "pubsub",
			Topic: o.Topic,
		},
	}

	// create context for clean shutdown
	c.ctx, c.cncl = context.WithCancel(context.Background())
	c.client, err = psb.NewSubscriberClient(c.ctx)

	path := fmt.Sprintf("projects/%s/subscriptions/%s", o.ProjectID, o.SubscriptionID)

	// Be sure to tune the MaxMessages parameter per your project's needs, and accordingly
	// adjust the ack behavior below to batch acknowledgements.
	c.pullReq = &ps.PullRequest{
		Subscription: path,
		MaxMessages:  1,
	}

	return c, err
}

// Msg never blocks, not sure if this is how it's supposed to work (needs testing)
func (c *Consumer) Msg() (msg []byte, done bool, err error) {
	if c.ctx.Err() != nil {
		// should not attempt to read if already stopped
		return msg, true, nil
	}

	resp, err := c.client.Pull(c.ctx, c.pullReq)
	if err != nil {
		return msg, done, err
	}

	c.info.Received++

	m := resp.ReceivedMessages[0]
	pm := m.GetMessage()
	msg = pm.Data

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
