package nsqbus

import (
	"fmt"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/pcelvng/task"
)

func TestNewConsumer(t *testing.T) {
	conf := &ConsumerConfig{
		Topic:            "testtopic",
		Channel:          "testchannel",
		NSQdTCPAddrs:     []string{"localhost:4151"},
		LookupdHTTPAddrs: []string{"localhost:4150"},
	}

	consumer := NewConsumer(conf)

	// check consumer
	if consumer == nil {
		t.Errorf("consumer should not be nil")
	}

	// check that conf was set by checking the topic
	if consumer.conf.Topic != conf.Topic {
		t.Errorf("expected topic '%v' but got '%v'", conf.Topic, consumer.conf.Topic)
	}
}

// TestConsumerNSQd will do a full test of this module.
// It will only run if nsqd is found in the local path.
//
// Tests:
// - Connects to local nsqd
func TestConsumerNSQd(t *testing.T) {
	// check for nsqd in the path
	_, err := exec.LookPath("nsqd")
	if err != nil {
		return // don't attempt full tests
	}

	// start nsqd
	cmd := exec.Command("nsqd")
	cmd.Start()
	defer cmd.Process.Signal(syscall.SIGINT)

	// Create Test Consumer
	conf := &ConsumerConfig{
		Topic:        "test-nsqconsumer",
		Channel:      "test-chan-nsqconsumer",
		NSQdTCPAddrs: []string{"localhost:4150"},
	}
	cons := NewConsumer(conf)

	// wait a sec to connect
	time.Sleep(time.Second * 2)

	// test Connect
	nsqCons, err := cons.Connect()
	defer nsqCons.Stop()
	if err != nil {
		t.Fatalf("%v", err)
	}

}

func NSQSimpleTaskProducer(topic string, tskCnt int) error {
	// create nsq consumer config
	nsqConf := nsq.NewConfig()
	producer, err := nsq.NewProducer("localhost:4150", nsqConf)
	if err != nil {
		return err
	}
	defer producer.Stop()

	// create tasks and publish
	body := make([]byte, 0)
	for i := 0; i < tskCnt; i++ {
		tskName := fmt.Sprintf("test-task-%v", i+1)
		tsk := task.New("test-task-type", tskName)
		body, err = tsk.Bytes()
		if err = producer.Publish(topic, body); err != nil {
			return err
		}
	}

	return nil
}

// TestConsumerLookupd will do a full test of this module.
// It will only run if nsqd and lookupd are found in the
// local path.
//
// Tests:
// - Connects to local lookupd and nsqd
func TestConsumerLookupd(t *testing.T) {
	// check for nsqd in the path

	// check for lookupd in the path
}
