package nsq

import (
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hydronica/trial"
	gonsq "github.com/nsqio/go-nsq"
)

func TestNewConsumer(t *testing.T) {
	if !nsqActive {
		t.Skip(skipNSQ)
	}
	logger := log.New(ioutil.Discard, "", 0)
	type input struct {
		topic   string
		channel string
		opts    Option
	}
	fn := func(in trial.Input) (interface{}, error) {
		v := in.Interface().(input)
		c, err := NewConsumer(v.topic, v.channel, &v.opts)
		isValid := c != nil
		if isValid && err == nil {
			err = c.Stop()
		}
		return isValid, err
	}
	cases := trial.Cases{
		"blank": {
			Input:     input{},
			ShouldErr: true,
			//Expected:  true,
		},
		"no topic": {
			Input:     input{channel: "testchannel"},
			ShouldErr: true,
		},
		"no channel": {
			Input:     input{topic: "testtopic"},
			ShouldErr: true,
		},
		"connect": {
			Input: input{
				topic:   "topic",
				channel: "test",
				opts:    Option{Logger: logger},
			},
			Expected: true,
		},
		"bad nsqds": { // bad port
			Input: input{
				topic:   "testtopic",
				channel: "test",
				opts:    Option{Logger: logger, NSQdAddrs: []string{"localhost:4000"}},
			},
			ShouldErr: true,
		},
		"nsqds": {
			Input: input{
				topic:   "topic",
				channel: "test",
				opts:    Option{Logger: logger, NSQdAddrs: []string{"localhost:4150"}},
			},
			Expected: true,
		},
	}
	trial.New(fn, cases).SubTest(t)
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
	if !nsqActive {
		t.Skip(skipNSQ)
	}
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

	if stats := c.consumer.Stats(); stats.Connections != 1 {
		t.Errorf("expected lookupd connections: %v", stats)
	}

	// check that consumer shuts down safely - even without
	// successfully connecting
	if err := c.Stop(); err != nil {
		t.Fatalf("bad shutdown: %v\n", err)
	}
}

func TestConsumer_Msg(t *testing.T) {
	if !nsqActive {
		t.Skip(skipNSQ)
	}
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
	if err := AddTasks(topic, msgCnt); err != nil {
		t.Fatal(err)
	}

	c, err := NewConsumer(topic, channel, opt)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(25 * time.Second)
		c.consumer.Stop()
	}()
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
