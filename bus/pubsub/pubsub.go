package pubsub

import (
	"log"
)

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

func NewOption(host, project, subscription, topic, jsonauth string, conn int) *Option {
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
