package nsq

import (
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNewProducer(t *testing.T) {
	conf := &Option{}
	p, err := NewProducer(conf)

	// err - not nil
	if err == nil {
		t.Error("expected err but got nil")
	}

	// producer - nil
	if p != nil {
		t.Error("producer should be nil")
	}
}

func TestProducer(t *testing.T) {
	// connect with bad address
	logger := log.New(ioutil.Discard, "", 0)
	conf := &Option{
		NSQdAddrs: []string{"localhost:4000"},
		Logger:    logger, // turn off nsq logging
	}
	p, err := NewProducer(conf)

	// err - not nil
	if err == nil {
		t.Error("expected err but got nil")
	}

	// producer - nil
	if p != nil {
		t.Error("producer should be nil")
	}

	// connect with good address
	conf = &Option{
		NSQdAddrs: []string{"127.0.0.1:4150"},
		Logger:    logger, // turn off nsq logging
	}
	p, err = NewProducer(conf)
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// send
	err = p.Send("test-producer-topic", []byte("test message"))

	// err - not nil
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// stop
	err = p.Stop()

	// err - nil
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// send
	err = p.Send("test-producer-topic", []byte("test message"))

	// err - not nil (producer already stopped)
	if err == nil {
		t.Error("expected err but got nil")
	}

	// stop
	err = p.Stop()

	// err - nil
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// connect to multiple good nsqds
	conf = &Option{
		NSQdAddrs: []string{"127.0.0.1:4150", "127.0.0.1:4150"},
		Logger:    logger, // turn off nsq logging
	}
	p, err = NewProducer(conf)

	// err - nil
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// send
	err = p.Send("test-producer-topic", []byte("test message"))

	// err - nil
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// connect to multiple good nsqds - one good one bad
	conf = &Option{
		NSQdAddrs: []string{"127.0.0.1:4150", "127.0.0.1:4000"},
		Logger:    logger, // turn off nsq logging
	}

	// should return an err since one host is bad
	p, err = NewProducer(conf)
	if err == nil {
		t.Error("expected err but got nil")
	}

	// p - nil
	if p != nil {
		t.Error("expected nil but got producer")
	}
}

func TestProducer_Race(t *testing.T) {
	// connect to multiple good nsqds
	topic := "test-producer-topic"
	msg := []byte("test message")
	logger := log.New(ioutil.Discard, "", 0)
	conf := &Option{
		NSQdAddrs: []string{"127.0.0.1:4150", "127.0.0.1:4150"},
		Logger:    logger, // turn off nsq logging
	}
	p, err := NewProducer(conf)

	// err - nil
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// send
	err = p.Send("test-producer-topic", []byte("test message"))

	// err - nil
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// TEST SERIAL CALLS
	//
	// Make a bunch of serial calls to test against
	// getting into a bad state.
	serialMsgCnt := 1000
	errCntGot := int64(0)
	for i := 0; i < serialMsgCnt; i++ {
		err := p.Send(topic, msg)
		if err != nil {
			atomic.AddInt64(&errCntGot, 1)
		}
	}

	expected := 0
	if int(errCntGot) != expected {
		t.Errorf("got '%v' errs but expected '%v'", errCntGot, expected)
	}

	// TEST CONCURRENT CALLS
	//
	// Create a bunch of go channels that will wait until
	// releaseChan is closed and then all call Send() at
	// the same time.
	pMsgCnt := 1000 // cnt of messages retrieved. Less than total messages loaded to topic.
	releaseChan := make(chan interface{})
	errCntGot = int64(0)
	wg := sync.WaitGroup{}
	for i := 0; i < pMsgCnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-releaseChan

			err := p.Send(topic, msg)
			if err != nil {
				atomic.AddInt64(&errCntGot, 1)
			}
		}()
	}

	// close channel to release all the Send() calls
	close(releaseChan)

	// wait for all messages to complete
	wg.Wait()

	expected = 0
	if int(errCntGot) != expected {
		t.Errorf("got '%v' errs but expected '%v'", errCntGot, expected)
	}

	// stop
	err = p.Stop()

	// err - nil
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}
}
