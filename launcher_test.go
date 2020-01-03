package task

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jbsmith7741/trial"
	"github.com/jbsmith7741/uri"

	"github.com/pcelvng/task/bus/nop"
)

type testWorker struct {
	waitTime time.Duration
}

func newTestWorker(info string) Worker {
	infoOpt := struct {
		WaitTime string `uri:"wait-time"`
	}{}
	uri.Unmarshal(info, &infoOpt)
	d, _ := time.ParseDuration(infoOpt.WaitTime)
	return &testWorker{waitTime: d}
}

func (t *testWorker) DoTask(ctx context.Context) (Result, string) {
	time.Sleep(t.waitTime)
	return CompleteResult, ""
}

func TestLauncher_Status(t *testing.T) {
	//setup
	prevMsg := nop.FakeMsg
	defer func() {
		nop.FakeMsg = prevMsg
	}()
	c, _ := nop.NewConsumer("")
	p, _ := nop.NewProducer("")

	// Test MaxInProgress is 100
	nop.FakeMsg = []byte(`{"type":"test","info":"?wait-time=5s"}`)
	l := NewLauncherFromBus(newTestWorker, c, p, &LauncherOptions{MaxInProgress: 100})
	l.DoTasks()
	time.Sleep(time.Millisecond)
	sts := l.Stats()
	if sts.TasksRunning != 100 {
		t.Errorf("Tasks running should be 100 !=%d", sts.TasksRunning)
	}

	// Test Average Run time - single worker
	nop.FakeMsg = []byte(`{"type":"test","info":"?wait-time=10ms"}`)
	l = NewLauncherFromBus(newTestWorker, c, p, nil)
	l.DoTasks()
	time.Sleep(100 * time.Millisecond)
	sts = l.Stats()
	if sts.MeanTaskTime != "10ms" {
		t.Errorf("Status should display MeanTaskTime: 10ms %v", sts.MeanTaskTime)
	}

	// Test Average Run time - multiple workers
	nop.FakeMsg = []byte(`{"type":"test","info":"?wait-time=10ms"}`)
	l = NewLauncherFromBus(newTestWorker, c, p, &LauncherOptions{MaxInProgress: 20})
	l.DoTasks()
	time.Sleep(100 * time.Millisecond)
	sts = l.Stats()
	if sts.MeanTaskTime != "10ms" {
		t.Errorf("Status should display MeanTaskTime: 10ms %v", sts.MeanTaskTime)
	}

}

type tMetaWorker struct{ Meta }

var _ meta = tMetaWorker{}

func (w tMetaWorker) DoTask(_ context.Context) (Result, string) {
	w.SetMeta("test-key", "value")
	return Completed("done")
}

func TestDoLaunch(t *testing.T) {
	launcher := &Launcher{
		stopCtx: context.Background(),
		lastCtx: context.Background(),
		slots:   make(chan int, 10),
		opt:     NewLauncherOptions(""),
		newWkr: func(_ string) Worker {
			return tMetaWorker{Meta: make(map[string][]string)}
		},
	}
	p, _ := nop.NewProducer("")
	launcher.wg.Add(1)
	launcher.producer = p
	tsk := &Task{}
	launcher.doLaunch(tsk)
	if err := json.Unmarshal([]byte(p.Messages["done"][0]), tsk); err != nil {
		t.Fatal(err)
	}
	tsk.Created, tsk.Started, tsk.Ended = "", "", ""
	tsk.started, tsk.ended = time.Time{}, time.Time{}
	eq, diff := trial.Equal(tsk, &Task{Result: "complete", Msg: "done", Meta: "test-key=value"})
	if !eq {
		t.Error(diff)
	}
}
