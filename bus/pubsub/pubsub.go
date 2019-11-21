package pubsub

import (
	"log"
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
