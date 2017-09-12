package simple

import (
	"bytes"
	"log"
	"os"
	"testing"
	"time"

	"github.com/pcelvng/task"
)

func TestNew(t *testing.T) {
	// test instantiation with no tasks
	var tsks []*task.Task
	receiver, err := New(tsks)
	if err == nil {
		t.Errorf("err should not be nil")
	}

	// test instantiation with two tasks
	tsks = []*task.Task{
		task.New("testtype", "testtask1"),
		task.New("testtype", "testtask2"),
	}

	receiver, err = New(tsks)
	if err != nil {
		t.Fatal(err)
	}

	// check that instantiated receiver is returned
	if receiver == nil {
		t.Errorf("receiver should not be nil")
	}

	// check that queue length is 2
	expectedCnt := 2
	if len(receiver.tsks) != expectedCnt {
		t.Errorf("expected %v tasks but got %v", expectedCnt, len(receiver.tsks))
	}
}

func TestNewFromFile(t *testing.T) {
	// test with a file of zero tasks
	receiver, err := NewFromFile("./test_tasks_empty.json")
	if err == nil {
		t.Errorf("err should not be nil")
	}

	// test with a file of two tasks
	receiver, err = NewFromFile("./test_tasks.json")
	if err != nil {
		t.Fatal(err)
	}

	// check that instantiated receiver is returned
	if receiver == nil {
		t.Errorf("receiver should not be nil")
	}

	// check that queue length is 2
	expectedCnt := 2
	if len(receiver.tsks) != expectedCnt {
		t.Errorf("expected %v tasks but got %v", expectedCnt, len(receiver.tsks))
	}
}

func TestFetchTasks(t *testing.T) {
	// test empty file
	tsks, err := fetchTasks("./test_tasks_empty.json")
	if err != nil {
		t.Fatal(err)
	}

	expected := 0
	if len(tsks) != expected {
		t.Errorf("got '%v' but expected '%v'", len(tsks), expected)
	}

	// test file with two records
	tsks, err = fetchTasks("./test_tasks.json")
	if err != nil {
		t.Fatal(err)
	}

	expected = 2
	if len(tsks) != expected {
		t.Errorf("got '%v' but expected '%v'", len(tsks), expected)
	}

	// test file with malformed json
	// if a single record is malformed then all the
	// good records should be discarded and the error
	// reported.
	tsks, err = fetchTasks("./test_tasks_malformed.json")
	if err == nil {
		t.Errorf("expected error but got '%v'", nil)
	}

	expected = 0
	if len(tsks) != expected {
		t.Errorf("got '%v' but expected '%v'", len(tsks), expected)
	}
}

func TestImplementsReceiver(t *testing.T) {
	// make sure implements the receiver interface
	// if it doesn't it will not compile.
	var _ task.Receiver = (*SimpleReceiver)(nil)
}

func TestSimpleReceiver_Next(t *testing.T) {
	// test a naked receiver next should also return error
	receiver := &SimpleReceiver{}
	tsk, last, err := receiver.Next()
	if tsk != nil {
		t.Errorf("expected nil task but got '%v'", tsk.Task)
	}

	// should be last
	if last {
		t.Errorf("task '%v' should not be the last task", tsk)
	}

	// should yield err
	if err == nil {
		t.Errorf("got error '%v'", err.Error())
	}

	// test with a couple tasks
	receiver.tsks = []*task.Task{
		task.New("testtype", "testtask1"),
		task.New("testtype", "testtask2"),
	}

	// should return a task, should not be last
	// and no error message.
	tsk, last, err = receiver.Next()
	expectedTask := "testtask1"
	if tsk.Task != expectedTask {
		t.Errorf("expected '%v' but got '%v'", expectedTask, tsk.Task)
	}

	// should not be last
	if last {
		t.Errorf("task '%v' should not be the last task", tsk.Task)
	}

	// should not yield err
	if err != nil {
		t.Errorf("got error '%v'", err.Error())
	}

	// should return a task, should be last
	// and no error message.
	tsk, last, err = receiver.Next()
	expectedTask = "testtask2"
	if tsk.Task != expectedTask {
		t.Errorf("expected '%v' but got '%v'", expectedTask, tsk.Task)
	}

	// should be last
	if !last {
		t.Errorf("task '%v' should be the last task", tsk.Task)
	}

	// should not yield err
	if err != nil {
		t.Errorf("got error '%v'", err.Error())
	}

	// should not return a task, should not be last
	// and have error message.
	tsk, last, err = receiver.Next()
	if tsk != nil {
		t.Errorf("expected nil task but got '%v'", tsk.Task)
	}

	// should be last
	if last {
		t.Errorf("task '%v' should not be the last task", tsk)
	}

	// should yield err
	if err == nil {
		t.Errorf("got error '%v'", err.Error())
	}
}

func TestSimpleReceiver_Done(t *testing.T) {
	// modify log buffer to check log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer log.SetOutput(os.Stderr)
	defer log.SetFlags(log.LstdFlags)

	tsk := task.New("testtype", "testtask1")
	ts := time.Date(2000, 01, 01, 00, 00, 00, 0, time.UTC)
	tsk.Timestamp = &ts
	receiver := &SimpleReceiver{}
	receiver.Done(tsk)

	logLine := buf.String()
	// expected output: `{"type":"testtype","task":"testtask1","timestamp":"2000-01-01T00:00:00Z"}`
	expectedCnt := 74
	if len(logLine) != expectedCnt {
		t.Errorf("expected '%v' but got '%v'", expectedCnt, len(logLine))
	}
}

func TestSimpleReceiver_Connect(t *testing.T) {
	receiver := &SimpleReceiver{}
	err := receiver.Connect()
	if err != nil {
		t.Errorf("expected '%v' but got '%v'", nil, err)
	}
}

func TestSimpleReceiver_Close(t *testing.T) {
	receiver := &SimpleReceiver{}
	err := receiver.Close()
	if err != nil {
		t.Errorf("expected '%v' but got '%v'", nil, err)
	}
}
