package nsq

import (
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNewProducer(t *testing.T) {
	conf := &Config{}
	p := NewProducer(conf)

	// test not nil
	if p == nil {
		t.Fatal("nsq producer should not be nil")
	}

	// test has conf
	if p.conf == nil {
		t.Error("nsq producer should have conf")
	}

	// test has nsqConf
	if p.nsqConf == nil {
		t.Error("nsq producer should have nsqConf")
	}

	// test has 3 conns
	expected := 3
	if p.numConns != expected {
		t.Errorf("expected '%v' numConss but got '%v'", expected, p.numConns)
	}

	// test Connect with no hosts
	err := p.Connect()
	if err == nil {
		t.Error("expected err but got nil")
	}
}

func TestProducer(t *testing.T) {
	// connect with bad address
	logger := log.New(ioutil.Discard, "", 0)
	conf := &Config{
		NSQdAddrs: []string{"localhost:4000"},
		Logger:    logger, // turn off nsq logging
	}
	p := NewProducer(conf)
	err := p.Connect()
	if err == nil {
		t.Error("expected err but got nil")
	}

	// connect with good address
	conf = &Config{
		NSQdAddrs: []string{"127.0.0.1:4150"},
		Logger:    logger, // turn off nsq logging
	}
	p = NewProducer(conf)

	// call send before connect to make sure it's safe
	err = p.Send("test-producer-topic", []byte("test message"))
	if err == nil {
		t.Error("expected err but got nil")
	}

	// call close before connect to make sure it's safe
	err = p.Close()
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	err = p.Connect()
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	err = p.Send("test-producer-topic", []byte("test message"))
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	err = p.Close()
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// connect to multiple good nsqds
	conf = &Config{
		NSQdAddrs: []string{"127.0.0.1:4150", "127.0.0.1:4150"},
		Logger:    logger, // turn off nsq logging
	}
	p = NewProducer(conf)

	err = p.Connect()
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	err = p.Send("test-producer-topic", []byte("test message"))
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// connect to multiple good nsqds - one good one bad
	conf = &Config{
		NSQdAddrs: []string{"127.0.0.1:4150", "127.0.0.1:4000"},
		Logger:    logger, // turn off nsq logging
	}
	p = NewProducer(conf)

	// should return an err since one host is bad
	err = p.Connect()
	if err == nil {
		t.Error("expected err but got nil")
	}

	// should return err since didn't connect correctly
	err = p.Send("test-producer-topic", []byte("test message"))
	if err == nil {
		t.Error("expected err but got nil")
	}
}

func TestProducer_Race(t *testing.T) {
	// connect to multiple good nsqds
	topic := "test-producer-topic"
	msg := []byte("test message")
	logger := log.New(ioutil.Discard, "", 0)
	conf := &Config{
		NSQdAddrs: []string{"127.0.0.1:4150", "127.0.0.1:4150"},
		Logger:    logger, // turn off nsq logging
	}
	p := NewProducer(conf)

	err := p.Connect()
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	err = p.Send("test-producer-topic", []byte("test message"))
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

	// close channel to release all the Msg() calls
	close(releaseChan)

	// wait for all messages to complete
	wg.Wait()

	expected = 0
	if int(errCntGot) != expected {
		t.Errorf("got '%v' errs but expected '%v'", errCntGot, expected)
	}
}
