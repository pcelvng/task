package io

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

var testFile = "test.txt"
var testParallelFile = "testparallel.txt"

func TestMain(m *testing.M) {
	exitCode := m.Run()

	// clean up file
	os.Remove(testFile)
	os.Remove(testParallelFile)

	os.Exit(exitCode)
}

func TestPkg(t *testing.T) {
	// create producer
	path := testFile
	p, err := NewFileProducer(path)
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}

	tmsg1 := []byte("test message 1")
	tmsg2 := []byte("test message 2")
	err = p.Send("", tmsg1)
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}

	err = p.Send("", tmsg2)
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}

	// create consumer
	c, err := NewFileConsumer(path)
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}

	// msg - no error
	msg1, done, err := c.Msg() // get first message
	if err != nil {
		t.Errorf("expected nil but got err: '%v'", err.Error())
	}

	expected := string(tmsg1)
	if string(msg1) != expected {
		t.Errorf("expected '%v' but got '%v'\n", expected, string(msg1))
	}

	// done - not done
	if done {
		t.Error("msg should not be marked done")
	}

	msg2, done, err := c.Msg()
	if err != nil {
		t.Errorf("expected nil but got '%v'\n", err.Error())
	} else {
		expected := string(tmsg2)
		if expected != string(msg2) {
			t.Errorf("expected '%v' but got '%v'\n", expected, string(msg1))
		}
	}
	if done {
		t.Error("msg should not be marked done")
	}

	msg3, done, err := c.Msg()
	if err != nil {
		t.Errorf("expected nil but got '%v'\n", err.Error())
	} else {
		expected := ""
		if expected != string(msg3) {
			t.Errorf("expected '%v' but got '%v'\n", expected, string(msg3))
		}
	}
	if !done {
		t.Error("msg should be marked done")
	}

	// test if Msg() is called after closing
	msg4, done, err := c.Msg()
	if err != nil {
		t.Errorf("expected nil but got '%v'\n", err.Error())
	} else {
		expected := ""
		if expected != string(msg4) {
			t.Errorf("expected '%v' but got '%v'\n", expected, string(msg4))
		}
	}
	if !done {
		t.Error("msg should be marked done")
	}
}

func TestParallel(t *testing.T) {
	// create producer
	path := testParallelFile
	p, err := NewFileProducer(path)
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}

	// load up messages in parallel
	pMsgCnt := 1000
	releaseChan := make(chan interface{})
	errCntGot := int64(0)
	wg := sync.WaitGroup{}
	for i := 0; i < pMsgCnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-releaseChan

			err := p.Send("", []byte("test message"))
			if err != nil {
				atomic.AddInt64(&errCntGot, 1)
			}
		}()
	}

	// close channel to release in parallel
	close(releaseChan)

	// wait for all messages to complete
	wg.Wait()

	expected := 0
	if int(errCntGot) != expected {
		t.Errorf("got '%v' errs but expected '%v'", errCntGot, expected)
	}

	// create consumer
	c, err := NewFileConsumer(path)
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}

	cMsgCnt := 1001
	releaseChan = make(chan interface{})
	errCntGot = int64(0)
	msgCntGot := int64(0)
	doneCntGot := int64(0)
	wg = sync.WaitGroup{}
	for i := 0; i < cMsgCnt; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-releaseChan

			msg, done, err := c.Msg()
			if err != nil {
				atomic.AddInt64(&errCntGot, 1)
			}

			if len(msg) > 0 {
				atomic.AddInt64(&msgCntGot, 1)
			}

			if done {
				atomic.AddInt64(&doneCntGot, 1)
			}
		}()
	}

	// close channel to release in parallel
	close(releaseChan)

	// wait for all messages to complete
	wg.Wait()

	expected = 0
	if int(errCntGot) != expected {
		t.Errorf("got '%v' err count but expected '%v'", errCntGot, expected)
	}

	expected = cMsgCnt - 1
	if int(msgCntGot) != expected {
		t.Errorf("got '%v' msg count but expected '%v'", msgCntGot, expected)
	}

	expected = 1
	if int(doneCntGot) != expected {
		t.Errorf("got '%v' done count but expected '%v'", doneCntGot, expected)
	}
}
