package nsqbus

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/pcelvng/task"
)

// startup will try to start nsq. If successful
// then nsqOn will bej set to true.
var (
	nsqdCmd    *exec.Cmd
	lookupdCmd *exec.Cmd
	logBuf     *bytes.Buffer // copy of buffer from changing log output
)

func TestMain(m *testing.M) {
	// start lookupd
	err := StartLookupd()
	if err != nil {
		log.Printf("unable to start lookupd: %v\n", err.Error())
		os.Exit(1)
	}

	// start nsqd
	// todo: require nsqd and lookupd to function for travis integration
	err = StartNsqd()
	if err != nil {
		fmt.Printf("unable to start nsq: %v\n", err.Error())
		os.Exit(1)
	}

	exitCode := m.Run()
	StopLookupd()
	StopNsqd()
	os.Exit(exitCode)
}

func logCaptureStart() {
	var buf bytes.Buffer

	log.SetOutput(&buf)
	log.SetFlags(0)
	logBuf = &buf
}

func logCaptureStop() *bytes.Buffer {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags)
	return logBuf
}

func TestNewConsumer(t *testing.T) {
	// turn off nsq client logging
	//logger := log.New(ioutil.Discard, "", 0)
	conf := &LazyConsumerConfig{
		Topic:   "testtopic",
		Channel: "testchannel",
		// Logger:       logger,
		// NSQdAddrs: []string{"localhost:4150"},
		// LookupdAddrs: []string{"localhost:4160"},
	}

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// check consumer
	if lc == nil {
		t.Errorf("consumer should not be nil")
	}

	// check the nsq consumer is nil (not set until Connect
	// is called)
	if lc.consumer != nil {
		t.Errorf("nsq consumer should be nil")
	}

	// check that conf was set by checking the topic
	if lc.conf.Topic != conf.Topic {
		t.Errorf("expected topic '%v' but got '%v'", conf.Topic, lc.conf.Topic)
	}

	// check that conf was set by checking the topic
	if lc.conf.Channel != conf.Channel {
		t.Errorf("expected channel '%v' but got '%v'", conf.Channel, lc.conf.Channel)
	}

	// check that nsqConf was set and that MaxInFlight == 0
	expected := 0
	if lc.nsqConf.MaxInFlight != expected {
		t.Errorf("expected channel '%v' but got '%v'", expected, lc.nsqConf.MaxInFlight)
	}

	// check that consumer shuts down safely - even without
	// having called Connect
	if err := lc.Close(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}

}

func TestLazyConsumer_ConnectNoTopic(t *testing.T) {
	// turn off nsq client logging
	//logger := log.New(ioutil.Discard, "", 0)
	conf := &LazyConsumerConfig{
		Topic:   "",
		Channel: "testchannel",
		// Logger:       logger,
		// NSQdAddrs: []string{"localhost:4150"},
		// LookupdAddrs: []string{"localhost:4160"},
	}

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local nsqd - should get invalid topic error
	if err := lc.Connect(); err == nil {
		t.Errorf("expected nsqd error but got nil instead\n")
	}

	// check nsq consumer (should still be nil)
	if lc.consumer != nil {
		t.Errorf("nsq consumer should be nil\n")
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := lc.Close(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}

}

func TestLazyConsumer_ConnectNoChannel(t *testing.T) {
	// turn off nsq client logging
	//logger := log.New(ioutil.Discard, "", 0)
	conf := &LazyConsumerConfig{
		Topic:   "testtopic",
		Channel: "",
		// Logger:       logger,
		// NSQdAddrs: []string{"localhost:4150"},
		// LookupdAddrs: []string{"localhost:4160"},
	}

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local nsqd - should get invalid topic error
	if err := lc.Connect(); err == nil {
		t.Errorf("expected nsqd invalid channel error but got nil instead\n")
	}

	// check nsq consumer (should still be nil)
	if lc.consumer != nil {
		t.Errorf("nsq consumer should be nil")
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := lc.Close(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestLazyConsumer_Connect(t *testing.T) {
	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	conf := &LazyConsumerConfig{
		Topic:   "testtopic",
		Channel: "testchannel",
		Logger:  logger,
		// NSQdAddrs: []string{"localhost:4150"},
		// LookupdAddrs: []string{"localhost:4160"},
	}

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local nsqd - should get invalid topic error
	if err := lc.Connect(); err != nil {
		t.Errorf("err '%v' when connecting to nsqd\n", err.Error())
	}

	// check nsq consumer (should still be nil)
	if lc.consumer == nil {
		t.Errorf("nsq consumer should not be nil")
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := lc.Close(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestLazyConsumer_ConnectNSQdsBad(t *testing.T) {
	// TEST BAD NSQD
	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	conf := &LazyConsumerConfig{
		Topic:     "testtopic",
		Channel:   "testchannel",
		Logger:    logger,
		NSQdAddrs: []string{"localhost:4000"},
		// LookupdAddrs: []string{"localhost:4160"},
	}

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local nsqd - should get invalid topic error
	if err := lc.Connect(); err == nil {
		t.Errorf("expected err but got nil\n")
	}

	// check nsq consumer (should still be nil)
	if lc.consumer != nil {
		t.Errorf("nsq consumer should be nil")
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := lc.Close(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestLazyConsumer_ConnectNSQds(t *testing.T) {
	// TEST GOOD NSQD
	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	conf := &LazyConsumerConfig{
		Topic:     "testtopic",
		Channel:   "testchannel",
		Logger:    logger,
		NSQdAddrs: []string{"localhost:4150"},
		// LookupdAddrs: []string{"localhost:4160"},
	}

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local nsqd - should get invalid topic error
	if err := lc.Connect(); err != nil {
		t.Errorf("expected nil but got err: '%v'\n", err.Error())
	}

	// check nsq consumer (should not be nil)
	if lc.consumer == nil {
		t.Errorf("nsq consumer should not be nil")
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := lc.Close(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestLazyConsumer_ConnectLookupdsBad(t *testing.T) {
	// TEST BAD LOOKUPD
	// Note: nsq will not return an error if a connection to
	// lookupd could not be made. It will instead keep trying
	// to connect. So, we don't expect an error in setting up
	// the nsq consumer but we do expect to see zero connections.

	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	conf := &LazyConsumerConfig{
		Topic:   "testtopic",
		Channel: "testchannel",
		Logger:  logger,
		// NSQdAddrs: []string{"localhost:4150"},
		LookupdAddrs: []string{"localhost:4000"}, // bad port
	}

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local lookupd - should get error
	if err := lc.Connect(); err != nil {
		t.Errorf("expected nil but got: '%v'\n", err.Error())
	}

	// check nsq consumer (should still be nil)
	if lc.consumer == nil {
		t.Errorf("nsq consumer should not be nil")
	}

	// check that there are no connections (since it's not a valid port)
	stats := lc.consumer.Stats()
	expected := 0
	if stats.Connections != expected {
		t.Errorf("expected '%v' but got '%v'", expected, stats.Connections)
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := lc.Close(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestLazyConsumer_ConnectLookupds(t *testing.T) {
	// TEST GOOD LOOKUPD
	// Note: nsq will not return an error if a connection to
	// lookupd could not be made. It will instead keep trying
	// to connect. With a good host:port combination we should see
	// at least one connection.

	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	conf := &LazyConsumerConfig{
		Topic:   "testtopic",
		Channel: "testchannel",
		Logger:  logger,
		// NSQdAddrs: []string{"localhost:4150"},
		LookupdAddrs: []string{"localhost:4161"}, // good port
	}

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local lookupd - should not get error
	if err := lc.Connect(); err != nil {
		t.Errorf("expected nil but got: '%v'\n", err.Error())
	}

	// check nsq consumer (should not be nil)
	if lc.consumer == nil {
		t.Errorf("nsq consumer should not be nil")
	}

	// check that there are no connections (since it's not a valid port)
	stats := lc.consumer.Stats()
	expected := 1
	if stats.Connections != expected {
		t.Errorf("expected '%v' but got '%v'", expected, stats.Connections)
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := lc.Close(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestLazyConsumer_Msg(t *testing.T) {
	// TEST MSG
	//
	// - Should not load any messages upon connecting
	// - Should lazy load a message.
	// - Should retrieve only one message per call
	// - Should not have messages in flight when Msg
	// has not been called.

	// load the topic with messages.
	topic := "testtopic"
	msgCnt := 5
	AddTasks(topic, msgCnt)

	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	conf := &LazyConsumerConfig{
		Topic:   topic,
		Channel: "testchannel",
		Logger:  logger,
		// NSQdAddrs: []string{"localhost:4150"},
		LookupdAddrs: []string{"localhost:4161"}, // good port
	}

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local lookupd - should not get error
	if err := lc.Connect(); err != nil {
		t.Errorf("expected nil but got: '%v'\n", err.Error())
	}

	// check that there is a connection
	stats := lc.consumer.Stats()
	expected := 1
	if stats.Connections != expected {
		t.Errorf("expected '%v' but got '%v'", expected, stats.Connections)
	}

	// no messages finished
	expected = 0
	if stats.MessagesFinished != uint64(expected) {
		t.Errorf("expected '%v' but got '%v'", expected, stats.MessagesFinished)
	}

	// no received messages
	expected = 0
	if stats.MessagesReceived != uint64(expected) {
		t.Errorf("expected '%v' but got '%v'", expected, stats.MessagesReceived)
	}

	// no re-queued messages
	expected = 0
	if stats.MessagesRequeued != uint64(expected) {
		t.Errorf("expected '%v' but got '%v'", expected, stats.MessagesRequeued)
	}

	// get one message
	b, err := lc.Msg()
	if err != nil {
		t.Errorf("expected nil but got '%v'", err.Error())
	}

	// should have some bytes
	if len(b) == 0 {
		t.Errorf("expected some bytes but didn't any")
	}

	// check that there is a connection
	stats = lc.consumer.Stats()

	// no messages finished
	expected = 1
	if stats.MessagesFinished != uint64(expected) {
		t.Errorf("expected '%v' but got '%v'", expected, stats.MessagesFinished)
	}

	// no received messages
	expected = 1
	if stats.MessagesReceived != uint64(expected) {
		t.Errorf("expected '%v' but got '%v'", expected, stats.MessagesReceived)
	}

	// no re-queued messages
	expected = 0
	if stats.MessagesRequeued != uint64(expected) {
		t.Errorf("expected '%v' but got '%v'", expected, stats.MessagesRequeued)
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := lc.Close(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
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

	// wait a sec for nsqd to start up
	time.Sleep(time.Millisecond * 500)

	return nil
}

func StopNsqd() error {
	if nsqdCmd != nil {
		nsqdCmd.Process.Signal(syscall.SIGINT)
	}
	if err := nsqdCmd.Wait(); err != nil {
		return err
	}

	// remove .dat files
	matches, err := filepath.Glob("*.dat")
	if err != nil {
		return err
	}

	for _, match := range matches {
		cmd := exec.Command("rm", match)
		cmd.Run()
	}

	return nil
}

func StartLookupd() error {
	// check for nsqd in the path
	_, err := exec.LookPath("nsqlookupd")
	if err != nil {
		return err // don't attempt full tests
	}

	// start nsqd
	cmd := exec.Command(
		"nsqlookupd",
		"--broadcast-address=127.0.0.1",
	)
	cmd.Start()
	lookupdCmd = cmd

	// wait a for lookupd to start up
	time.Sleep(time.Millisecond * 500)

	return nil
}

func StopLookupd() error {
	if lookupdCmd != nil {
		lookupdCmd.Process.Signal(syscall.SIGINT)
	}

	if err := lookupdCmd.Wait(); err != nil {
		return err
	}

	return nil
}

func AddTasks(topic string, tskCnt int) error {
	// create nsq consumer config
	nsqConf := nsq.NewConfig()

	producer, err := nsq.NewProducer("localhost:4150", nsqConf)
	if err != nil {
		return err
	}
	defer producer.Stop()

	// disable logging
	logger := log.New(ioutil.Discard, "", 0)
	producer.SetLogger(logger, nsq.LogLevelInfo)

	// create tasks and publish
	body := make([]byte, 0)
	for i := 0; i < tskCnt; i++ {
		tsk := task.New("test-task-type", "test-task")
		body, err = tsk.Bytes()
		if err = producer.Publish(topic, body); err != nil {
			return err
		}
	}

	return nil
}
