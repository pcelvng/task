package nsq

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gonsq "github.com/nsqio/go-nsq"
)

// TestMain will setup nsqd and lookupd. It expects those two binaries
// to exist in PATH or the tests will fail.
//
// WEIRD ISSUE: when I run this test in Gogland the then switch
// to a terminal to curl nsqd (to check the stats) or visa versa then
// sometimes the test run will freeze for a long time and then fail with
// very strange results.
// This did not happen in doing the same thing between two terminal
// instances.
func TestMain(m *testing.M) {
	// start lookupd
	err := StartLookupd()
	if err != nil {
		log.Printf("unable to start lookupd: %v\n", err.Error())
		os.Exit(1)
	}

	// start nsqd
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

// startup will try to start nsq. If successful
// then nsqOn will bej set to true.
var (
	nsqdCmd    *exec.Cmd
	lookupdCmd *exec.Cmd

	nsqdPath             = "./test/nsqd"
	nsqlookupdPath       = "./test/nsqlookupd"
	darwinNsqdPath       = "./test/darwin_nsqd"
	darwinNsqlookupdPath = "./test/darwin_nsqlookupd"
)

func TestNewConsumer(t *testing.T) {
	// turn off nsq client logging
	//logger := log.New(ioutil.Discard, "", 0)
	opt := &Option{
	// Logger:       logger,
	// NSQdAddrs: []string{"localhost:4150"},
	// LookupdAddrs: []string{"localhost:4160"},
	}

	c, err := NewConsumer("", "", opt)
	if err == nil {
		t.Fatal("err should not be nil")
	}

	// consumer is nil
	if c != nil {
		t.Error("consumer should be nil")
	}
}

func TestConsumer_ConnectNoTopic(t *testing.T) {
	// turn off nsq client logging
	//logger := log.New(ioutil.Discard, "", 0)
	opt := &Option{
	// Logger:       logger,
	// NSQdAddrs: []string{"localhost:4150"},
	// LookupdAddrs: []string{"localhost:4160"},
	}
	topic := ""
	channel := "testchannel"

	c, err := NewConsumer(topic, channel, opt)

	// err is not nil - invalid topic name
	if err == nil {
		t.Fatal("expected err but got nil")
	}

	// consumer is nil
	if c != nil {
		t.Error("consumer should be nil")
	}
}

func TestConsumer_ConnectNoChannel(t *testing.T) {
	// turn off nsq client logging
	//logger := log.New(ioutil.Discard, "", 0)
	opt := &Option{
	// Logger:       logger,
	// NSQdAddrs: []string{"localhost:4150"},
	// LookupdAddrs: []string{"localhost:4160"},
	}
	topic := "testtopic"
	channel := ""

	c, err := NewConsumer(topic, channel, opt)

	// err is not nil - invalid channel name
	if err == nil {
		t.Fatal("expected err but got nil")
	}

	// consumer is nil
	if c != nil {
		t.Error("consumer should be nil")
	}
}

func TestConsumer_Connect(t *testing.T) {
	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	opt := &Option{
		Logger: logger,
		// NSQdAddrs: []string{"localhost:4150"},
		// LookupdAddrs: []string{"localhost:4160"},
	}
	topic := "testtopic"
	channel := "testchannel"

	c, err := NewConsumer(topic, channel, opt)

	// err - nil
	if err != nil {
		t.Errorf("err should be nil got '%v' instead\n", err.Error())
	}

	// consumer - not nil
	if c == nil {
		t.Fatal("consumer should not be nil")
	}

	// c.consumer - not nil
	if c.consumer == nil {
		t.Fatal("nsq consumer should not be nil")
	}

	// stop - no error
	if err := c.Stop(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestConsumer_ConnectNSQdsBad(t *testing.T) {
	// TEST BAD NSQD
	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	opt := &Option{
		Logger:    logger,
		NSQdAddrs: []string{"localhost:4000"},
		// LookupdAddrs: []string{"localhost:4160"},
	}
	topic := "testtopic"
	channel := "testchannel"

	c, err := NewConsumer(topic, channel, opt)

	// err - not nil
	if err == nil {
		t.Fatal("expected err but got nil")
	}

	// consumer - nil
	if c != nil {
		t.Error("consumer should be nil")
	}
}

func TestConsumer_ConnectNSQds(t *testing.T) {
	// TEST GOOD NSQD
	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	opt := &Option{
		Logger:    logger,
		NSQdAddrs: []string{"localhost:4150"},
		// LookupdAddrs: []string{"localhost:4160"},
	}
	topic := "testtopic"
	channel := "testchannel"

	c, err := NewConsumer(topic, channel, opt)
	if err != nil {
		t.Fatal(err)
	}

	// check nsq consumer (should not be nil)
	if c.consumer == nil {
		t.Errorf("nsq consumer should not be nil")
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := c.Stop(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestConsumer_ConnectLookupdsBad(t *testing.T) {
	// TEST BAD LOOKUPD
	// Note: nsq will not return an error if a connection to
	// lookupd could not be made. It will instead keep trying
	// to connect. So, we don't expect an error in setting up
	// the nsq consumer but we do expect to see zero connections.

	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	opt := &Option{
		Logger: logger,
		// NSQdAddrs: []string{"localhost:4150"},
		LookupdAddrs: []string{"localhost:4000"}, // bad port
	}
	topic := "testtopic"
	channel := "testchannel"

	c, err := NewConsumer(topic, channel, opt)
	if err != nil {
		t.Fatal(err)
	}

	// check nsq consumer (should still be nil)
	if c.consumer == nil {
		t.Errorf("nsq consumer should not be nil")
	}

	// check that there are no connections (since it's not a valid port)
	stats := c.consumer.Stats()
	expected := 0
	if stats.Connections != expected {
		t.Errorf("expected '%v' but got '%v'", expected, stats.Connections)
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := c.Stop(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestConsumer_ConnectLookupds(t *testing.T) {
	// TEST GOOD LOOKUPD
	// Note: nsq will not return an error if a connection to
	// lookupd could not be made. It will instead keep trying
	// to connect. With a good host:port combination we should see
	// at least one connection.

	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	opt := &Option{
		Logger: logger,
		// NSQdAddrs: []string{"localhost:4150"},
		LookupdAddrs: []string{"localhost:4161"}, // good port
	}
	topic := "testtopic"
	channel := "testchannel"

	c, err := NewConsumer(topic, channel, opt)
	if err != nil {
		t.Fatal(err)
	}

	// check nsq consumer (should not be nil)
	if c.consumer == nil {
		t.Errorf("nsq consumer should not be nil")
	}

	// check that there are no connections (since it's not a valid port)
	stats := c.consumer.Stats()
	expected := 1
	if stats.Connections != expected {
		t.Errorf("expected '%v' but got '%v'", expected, stats.Connections)
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := c.Stop(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestConsumer_Msg(t *testing.T) {
	// TEST MSG
	//
	// - Should not load any messages upon connecting
	// - Should lazy load a message
	// - Should retrieve only one message per call
	// - Should not have messages in flight when Msg
	// has not been called
	// - Should be able to handle multiple concurrent Msg()
	// calls.

	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	opt := &Option{
		Logger: logger,
		// NSQdAddrs: []string{"localhost:4150"},
		LookupdAddrs: []string{"localhost:4161"}, // good port
	}
	topic := "testtopic"
	channel := "testchannel"

	msgCnt := 1000
	AddTasks(topic, msgCnt)

	c, err := NewConsumer(topic, channel, opt)
	if err != nil {
		t.Fatal(err)
	}

	// check that there is a connection
	stats := c.consumer.Stats()
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
	b, _, err := c.Msg()
	if err != nil {
		t.Errorf("expected nil but got '%v'", err.Error())
	}

	// should have some bytes
	if len(b) == 0 {
		t.Errorf("expected some bytes but didn't any")
	}

	tckr := time.NewTicker(time.Millisecond * 10)
	<-tckr.C

	// check that there is a connection
	stats = c.consumer.Stats()

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

	// TEST SERIAL CALLS
	//
	// Make a bunch of serial calls to test against
	// getting into a bad state.
	serialMsgCnt := 100
	errCntGot := int64(0)
	msgCntGot := int64(0)
	for i := 0; i < serialMsgCnt; i++ {
		b, _, err := c.Msg()
		if err != nil {
			atomic.AddInt64(&errCntGot, 1)
		} else if len(b) > 0 {
			atomic.AddInt64(&msgCntGot, 1)
		} else {
			t.Fatalf("msg has zero byte length '%v'\n", string(b))
		}
	}

	expected = 0
	if int(errCntGot) != expected {
		t.Errorf("got '%v' errs but expected '%v'", errCntGot, expected)
	}

	expected = serialMsgCnt
	if int(msgCntGot) != expected {
		t.Errorf("got '%v' msgs but expected '%v'", msgCntGot, expected)
	}

	// check the consumer stats again
	tckr = time.NewTicker(time.Millisecond * 5)
	<-tckr.C
	stats = c.consumer.Stats()

	// messages finished
	expected = 1 + serialMsgCnt
	if stats.MessagesFinished != uint64(expected) {
		t.Errorf("expected '%v' but got '%v'", expected, stats.MessagesFinished)
	}

	// received messages
	expected = 1 + serialMsgCnt
	if stats.MessagesReceived != uint64(expected) {
		t.Errorf("expected '%v' but got '%v'", expected, stats.MessagesReceived)
	}

	// no re-queued messages
	maxExpected := 1
	if stats.MessagesRequeued > uint64(maxExpected) {
		t.Errorf("expected at most '%v' but got '%v'", maxExpected, stats.MessagesRequeued)
	}

	// TEST CONCURRENT CALLS
	//
	// Create a bunch of go channels that will wait until
	// releaseChan is closed and then all call Msg() at
	// the same time.
	pMsgCnt := 300 // cnt of messages retrieved. Less than total messages loaded to topic.
	releaseChan := make(chan interface{})
	errCntGot = int64(0)
	msgCntGot = int64(0)
	wg := sync.WaitGroup{}
	for i := 0; i < pMsgCnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-releaseChan

			b, _, err := c.Msg()
			if err != nil {
				atomic.AddInt64(&errCntGot, 1)
			} else if len(b) > 0 {
				atomic.AddInt64(&msgCntGot, 1)
			} else {
				t.Fatalf("msg has zero byte length '%v'\n", string(b))
			}
		}()
	}

	// close channel to release all the Msg() calls
	close(releaseChan)

	// wait for all messages to complete
	wg.Wait()

	expected = 0
	if int(errCntGot) != expected {
		t.Errorf("got '%v' errs but expected '%v'", errCntGot, expected)
	}

	expected = pMsgCnt
	if int(msgCntGot) != expected {
		t.Errorf("got '%v' msgs but expected '%v'", msgCntGot, expected)
	}

	// check the consumer stats again
	tckr = time.NewTicker(time.Millisecond * 5)
	<-tckr.C
	stats = c.consumer.Stats()

	// messages finished
	expected = 1 + pMsgCnt + serialMsgCnt
	if stats.MessagesFinished != uint64(expected) {
		t.Errorf("expected '%v' but got '%v'", expected, stats.MessagesFinished)
	}

	// received messages
	expected = 1 + pMsgCnt + serialMsgCnt
	if stats.MessagesReceived != uint64(expected) {
		t.Errorf("expected '%v' but got '%v'", expected, stats.MessagesReceived)
	}

	// no re-queued messages
	maxExpected = 1
	if stats.MessagesRequeued > uint64(maxExpected) {
		t.Errorf("expected at most '%v' but got '%v'", maxExpected, stats.MessagesRequeued)
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := c.Stop(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}

}

func StartNsqd() error {
	// check for nsqd in the path
	var err error
	if runtime.GOOS == "darwin" {
		_, err = exec.LookPath(darwinNsqdPath)
	} else {
		_, err = exec.LookPath(nsqdPath)
	}
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
	tckr := time.NewTicker(time.Millisecond * 50)
	<-tckr.C
	return nil
}

func StopNsqd() error {
	if nsqdCmd != nil {
		nsqdCmd.Process.Signal(syscall.SIGINT)
	}
	nsqdCmd.Wait()

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
	// check for nsqlookupd in the path
	var err error
	if runtime.GOOS == "darwin" {
		_, err = exec.LookPath(darwinNsqlookupdPath)
	} else {
		_, err = exec.LookPath(nsqlookupdPath)
	}
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
	tckr := time.NewTicker(time.Millisecond * 50)
	<-tckr.C

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
	nsqConf := gonsq.NewConfig()

	producer, err := gonsq.NewProducer("localhost:4150", nsqConf)
	if err != nil {
		return err
	}
	defer producer.Stop()

	// disable logging
	logger := log.New(ioutil.Discard, "", 0)
	producer.SetLogger(logger, gonsq.LogLevelInfo)

	// create tasks and publish
	body := []byte(`{"type":"test-task-type","info":"test-task"}`)
	for i := 0; i < tskCnt; i++ {
		if err = producer.Publish(topic, body); err != nil {
			return err
		}
	}

	return nil
}
