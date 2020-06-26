package pubsub

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hydronica/trial"
)

var skipPubsub bool
var skipText = "\x1b[1;34mSKIP: pubsub not running\x1b[0m"

func TestMain(t *testing.M) {
	opts := &Option{Host: "127.0.0.1:8085"}
	_, err := Topics(opts)
	if err != nil {
		log.Println("pubsub not running", err)
		skipPubsub = true
	}
	t.Run()
}

func TestNewConsumer(t *testing.T) {
	if skipPubsub {
		t.Skip(skipText)
	}
	fn := func(in trial.Input) (interface{}, error) {
		opts := in.Interface().(*Option)
		os.Setenv("PUBSUB_EMULATOR_HOST", "")

		c, err := opts.NewConsumer()
		return c != nil, err
	}

	cases := trial.Cases{
		"local host": {
			Input:    NewOption("127.0.0.1:8085", "test", "topic1-sub", "topic", ""),
			Expected: true,
		},
		"missing topic": {
			Input: &Option{
				Host: "127.0.0.1:8085",
			},
			ShouldErr: true,
		},
		"missing subscription": {
			Input: &Option{
				Host:  "127.0.0.1:8085",
				Topic: "topic1",
			},
			ShouldErr: true,
		},
		"invalid": {
			Input:     &Option{Host: ""},
			ShouldErr: true,
		},
	}

	trial.New(fn, cases).Test(t)
}

func TestTopics(t *testing.T) {
	if skipPubsub {
		t.Skip(skipText)
	}
	opts := &Option{Host: "127.0.0.1:8085"}
	s, err := Topics(opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(s) > 0 {
		t.Logf("PASS: %v", s)
	}
}

func TestProducer(t *testing.T) {
	if skipPubsub {
		t.Skip(skipText)
	}
	opts := &Option{Host: "127.0.0.1:8085"}
	producer, err := opts.NewProducer()
	if err != nil {
		log.Fatal("new", err)
	}
	if err := producer.Stop(); err != nil {
		log.Fatal("stop", err)
	}

	// test sending on stopped producer
	tName := "stopped producer"
	if err := producer.Send("topic1", []byte("hello world")); err == nil {
		t.Logf("FAIL: %q expected error ", tName)
	} else if !strings.Contains(err.Error(), "already stopped") {
		t.Errorf("FAIL: %q unexpected error %s", tName, err)
	} else {
		t.Logf("PASS: %q", tName)
	}

	// send on uncreated topic
	tName = "send"
	producer, _ = opts.NewProducer()
	if err := producer.Send("topic2", []byte("hello")); err != nil {
		t.Errorf("FAIL: %q %s", tName, err)
	} else {
		t.Logf("PASS: %q", tName)
	}

	// send with no topic
	tName = "blank topic"
	if err := producer.Send("", []byte("empty")); err == nil {
		t.Errorf("FAIL: %q expected error", tName)
	} else {
		t.Logf("PASS: %q", tName)
	}

}

func TestConsumer_Msg(t *testing.T) {
	if skipPubsub {
		t.Skip(skipText)
	}
	// setup
	opts := &Option{
		Host:         "127.0.0.1:8085",
		Topic:        "topic1",
		Subscription: "topic1-sub",
	}
	consumer, err := opts.NewConsumer()
	if err != nil {
		t.Fatal("consumer", err)

	}
	producer, err := opts.NewProducer()
	if err != nil {
		t.Fatal("producer", err)
	}
	// ------- tests --------
	// msg should return done when consumer is stopped
	tName := "stopped consumer"
	if err := consumer.Stop(); err != nil {
		t.Errorf("FAIL: %q %s", tName, err)
	}
	_, done, err := consumer.Msg()
	if err != nil {
		t.Errorf("FAIL: %q %s", tName, err)
	} else if !done {
		t.Errorf("FAIL: %q expected done", tName)
	} else {
		t.Logf("PASS: %q", tName)
	}

	// publisher messages
	msgCount := 10
	for i := 0; i < msgCount; i++ {
		msg := strconv.Itoa(i)
		if err := producer.Send(opts.Topic, []byte(msg)); err != nil {
			t.Fatal("producer send", err)
		}
	}

	consumer, _ = opts.NewConsumer()
	consumer.ctx, _ = context.WithTimeout(consumer.ctx, 5*time.Second)
	tName = "read messages"
	msgs := make([]string, 0)
	for i := 0; i < msgCount; i++ {
		b, done, err := consumer.Msg()
		if err != nil {
			t.Fatalf("FATAL: %q %s", tName, err)
		} else if done {
			t.Fatalf("FAIL: %q %d/%d messages read", tName, i, msgCount)
		}
		msgs = append(msgs, string(b))
	}
	t.Logf("PASS: %q %v", tName, msgs)

	consumer.Stop()
	producer.Stop()
}
