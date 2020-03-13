package pubsub

import (
	"context"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Option are the settings to connect to a pubsub project instance.
type Option struct {
	// host should only be set for emulator
	Host           string `uri:"host"`
	ProjectID      string `uri:"project"`
	SubscriptionID string `uri:"subscription"`
	Topic          string `uri:"topic" required:"true"`
	JSONAuth       string `uri:"jsonauth"`
	// if nil then the default nsq logger is used
	Logger *log.Logger
}

// NewOption creates a set of default options for pubsub,
// host is only used for the fake emulator
// project is the google project (id) name
// subscription is the name of the pubsub subscription will be created if it doesn't exit
// topic is the pubsub topic, will be created if it doesn't exist
// jsonauth is the generated json string authorization settings for access to pubsub
func NewOption(host, project, subscription, topic, jsonauth string) *Option {
	o := &Option{
		Host:           "", // only used for the emulator
		ProjectID:      "project.id",
		SubscriptionID: "default.topic.channel",
		Topic:          "default.topic",
		JSONAuth:       "",
	}

	if project != "" {
		o.ProjectID = project
	}

	if subscription != "" {
		o.SubscriptionID = subscription
	}

	if topic != "" {
		o.Topic = topic
	}

	o.Host = host
	o.JSONAuth = jsonauth

	return o
}

func (o *Option) newClient() (*pubsub.Client, error) {
	opts := make([]option.ClientOption, 0)
	if o.Host != "" && o.Host != "/" {
		os.Setenv("PUBSUB_EMULATOR_HOST", o.Host)
	}

	if o.JSONAuth != "" {
		opts = append(opts, option.WithCredentialsFile(o.JSONAuth))
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	return pubsub.NewClient(ctx, o.ProjectID, opts...)
}

func Topics(o *Option) ([]string, error) {
	client, err := o.newClient()
	defer client.Close()
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	q := client.Topics(ctx)
	topics := make([]string, 0)

	for t, err := q.Next(); err != iterator.Done; t, err = q.Next() {
		if err != nil {
			return nil, err
		}
		topics = append(topics, t.ID())
	}

	return topics, err
}
