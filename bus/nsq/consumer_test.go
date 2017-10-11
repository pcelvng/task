package nsqbus

import (
	"fmt"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/pcelvng/task"
	"os"
)

// startup will try to start nsq. If successful
// then nsqOn will be set to true.
var (
	nsqdOn = false
	nsqdCmd *exec.Cmd
	lookupdOn = false
	lookupdCmd *exec.Cmd
)


func TestMain(m *testing.M) {
	// start nsqd
	err := StartNsqd()
	if err != nil {
		fmt.Printf("unable to start nsq: %v", err.Error())
		os.Exit(1)
	}
	nsqdOn = true

	// start lookupd
	err = StartLookupd()
	if err != nil {
		fmt.Printf("unable to start lookupd: %v", err.Error())
	}
	lookupdOn = true

	exitCode := m.Run()
	StopLookupd()
	StopNsqd()
	os.Exit(exitCode)
}

func TestNewConsumer(t *testing.T) {
	conf := &LazyConsumerConfig{
		Topic:            "testtopic",
		Channel:          "testchannel",
		NSQdTCPAddrs:     []string{"localhost:4150"},
		LookupdHTTPAddrs: []string{"localhost:4160"},
	}

	consumer, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// check consumer
	if consumer == nil {
		t.Errorf("consumer should not be nil")
	}

	// check that conf was set by checking the topic
	if consumer.conf.Topic != conf.Topic {
		t.Errorf("expected topic '%v' but got '%v'", conf.Topic, consumer.conf.Topic)
	}

	// Create Test Consumer
	//conf := &LazyConsumerConfig{
	//	Topic:        "test-nsqconsumer",
	//	Channel:      "test-chan-nsqconsumer",
	//	NSQdTCPAddrs: []string{"localhost:4150"},
	//}
	//cons, err := NewLazyConsumer(conf)
	//if err != nil {
	//	return err
	//}
}

// TestConsumerNSQd will do a full test of this module.
// It will only run if nsqd is found in the local path.
//
// Tests:
// - Connects to local nsqd
func TestConsumerNSQd(t *testing.T) {


}

func StartNsqd() error {
	// check for nsqd in the path
	_, err := exec.LookPath("nsqd")
	if err != nil {
		return err // don't attempt full tests
	}

	// start nsqd
	cmd := exec.Command(
		"nsqd",
		"--lookupd-tcp-address=127.0.0.1:4160",
		"--http-address=127.0.0.1:4151",
		"--tcp-address=127.0.0.1:4150",
		"--broadcast-address=127.0.0.1",
	)
	cmd.Start()
	nsqdCmd = cmd

	// wait a sec to connect
	time.Sleep(time.Second * 2)

	return nil
}

func StopNsqd() {
	nsqdCmd.Process.Signal(syscall.SIGINT)
}

func StartLookupd() {
	// nsqlookupd --broadcast-address=127.0.0.1

}

func NsqSimpleTaskProducer(topic string, tskCnt int) error {
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
