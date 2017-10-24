package nsq

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gonsq "github.com/bitly/go-nsq"
	"github.com/pcelvng/task"
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
	conf := &Config{
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
	conf := &Config{
	// Logger:       logger,
	// NSQdAddrs: []string{"localhost:4150"},
	// LookupdAddrs: []string{"localhost:4160"},
	}
	topic := ""
	channel := "testchannel"

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local nsqd - should get invalid topic error
	if err := lc.Connect(topic, channel); err == nil {
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
	conf := &Config{
		// Logger:       logger,
		// NSQdAddrs: []string{"localhost:4150"},
		// LookupdAddrs: []string{"localhost:4160"},
	}
	topic := "testtopic"
	channel := ""

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local nsqd - should get invalid topic error
	if err := lc.Connect(topic, channel); err == nil {
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
	conf := &Config{
		Logger:  logger,
		// NSQdAddrs: []string{"localhost:4150"},
		// LookupdAddrs: []string{"localhost:4160"},
	}
	topic := "testtopic"
	channel := "testchannel"

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local nsqd - should get invalid topic error
	if err := lc.Connect(topic, channel); err != nil {
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
	conf := &Config{
		Logger:    logger,
		NSQdAddrs: []string{"localhost:4000"},
		// LookupdAddrs: []string{"localhost:4160"},
	}
	topic := "testtopic"
	channel := "testchannel"

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local nsqd - should get invalid topic error
	if err := lc.Connect(topic, channel); err == nil {
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
	conf := &Config{
		Logger:    logger,
		NSQdAddrs: []string{"localhost:4150"},
		// LookupdAddrs: []string{"localhost:4160"},
	}
	topic := "testtopic"
	channel := "testchannel"

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local nsqd - should get invalid topic error
	if err := lc.Connect(topic, channel); err != nil {
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
	conf := &Config{
		Logger:  logger,
		// NSQdAddrs: []string{"localhost:4150"},
		LookupdAddrs: []string{"localhost:4000"}, // bad port
	}
	topic := "testtopic"
	channel := "testchannel"

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local lookupd - should get error
	if err := lc.Connect(topic, channel); err != nil {
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
	conf := &Config{
		Logger:  logger,
		// NSQdAddrs: []string{"localhost:4150"},
		LookupdAddrs: []string{"localhost:4161"}, // good port
	}
	topic := "testtopic"
	channel := "testchannel"

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local lookupd - should not get error
	if err := lc.Connect(topic, channel); err != nil {
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
	// - Should lazy load a message
	// - Should retrieve only one message per call
	// - Should not have messages in flight when Msg
	// has not been called
	// - Should be able to handle multiple concurrent Msg()
	// calls.

	// turn off nsq client logging
	logger := log.New(ioutil.Discard, "", 0)
	conf := &Config{
		Logger:  logger,
		// NSQdAddrs: []string{"localhost:4150"},
		LookupdAddrs: []string{"localhost:4161"}, // good port
	}
	topic := "testtopic"
	channel := "testchannel"

	msgCnt := 1000
	AddTasks(topic, msgCnt)

	lc, err := NewLazyConsumer(conf)
	if err != nil {
		t.Fatal(err)
	}

	// connect to local lookupd - should not get error
	if err := lc.Connect(topic, channel); err != nil {
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
	b, _, err := lc.Msg()
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

	// TEST SERIAL CALLS
	//
	// Make a bunch of serial calls to test against
	// getting into a bad state.
	serialMsgCnt := 100
	errCntGot := int64(0)
	msgCntGot := int64(0)
	for i := 0; i < serialMsgCnt; i++ {
		b, _, err := lc.Msg()
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
	stats = lc.consumer.Stats()

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

			b, _, err := lc.Msg()
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
	stats = lc.consumer.Stats()

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
	if err := lc.Close(); err != nil {
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

func getNSQdStats(topic, channel string) (*NsqdStats, error) {
	resp, err := http.Get("http://localhost:4151/stats?format=json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	// deserialize JSON
	stats := &NsqdStats{}
	if err := json.Unmarshal(body, stats); err != nil {
		return nil, err
	}

	return stats, nil
}

type NsqdStats struct {
	Version   string `json:"version"`
	Health    string `json:"health"`
	StartTime int    `json:"start_time"`
	Topics    []struct {
		TopicName            string        `json:"topic_name"`
		Channels             []interface{} `json:"channels"`
		Depth                int           `json:"depth"`
		BackendDepth         int           `json:"backend_depth"`
		MessageCount         int           `json:"message_count"`
		Paused               bool          `json:"paused"`
		E2EProcessingLatency struct {
			Count       int         `json:"count"`
			Percentiles interface{} `json:"percentiles"`
		} `json:"e2e_processing_latency"`
	} `json:"topics"`
	Memory struct {
		HeapObjects       int `json:"heap_objects"`
		HeapIdleBytes     int `json:"heap_idle_bytes"`
		HeapInUseBytes    int `json:"heap_in_use_bytes"`
		HeapReleasedBytes int `json:"heap_released_bytes"`
		GcPauseUsec100    int `json:"gc_pause_usec_100"`
		GcPauseUsec99     int `json:"gc_pause_usec_99"`
		GcPauseUsec95     int `json:"gc_pause_usec_95"`
		NextGcBytes       int `json:"next_gc_bytes"`
		GcTotalRuns       int `json:"gc_total_runs"`
	} `json:"memory"`
}
