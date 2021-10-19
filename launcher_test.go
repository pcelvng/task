package task

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/hydronica/trial"
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
	time.Sleep(100 * time.Millisecond)
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

type tMetaWorker struct {
	Info string
	Meta
}

var _ meta = tMetaWorker{}

func (w tMetaWorker) DoTask(_ context.Context) (Result, string) {
	u, err := url.Parse(w.Info)
	if err != nil {
		return Failed(err)
	}
	for k, v := range u.Query() {
		w.Meta.SetMeta(k, v...)
	}

	return Completed("done")
}

func TestDoLaunch(t *testing.T) {
	wrkFn := func(info string) Worker {
		return tMetaWorker{Info: info, Meta: make(map[string][]string)}
	}
	type input struct {
		meta string
		info string
	}
	fn := func(i trial.Input) (interface{}, error) {
		// Create a worker that converts converts all info data
		// into meta. This test is designed to test maintaining
		// the meta data across tasks without duplicating them.
		in := i.Interface().(input)
		p, err := nop.NewProducer("")
		if err != nil {
			return nil, err
		}

		launcher := &Launcher{
			stopCtx:  context.Background(),
			lastCtx:  context.Background(),
			slots:    make(chan int, 10),
			opt:      NewLauncherOptions(""),
			producer: p,
			newWkr:   wrkFn,
		}
		launcher.wg.Add(1)
		tsk := &Task{Info: in.info, Meta: in.meta}
		launcher.doLaunch(tsk)
		if err := json.Unmarshal([]byte(p.Messages["done"][0]), tsk); err != nil {
			return nil, err
		}
		return tsk.Meta, nil
	}
	cases := trial.Cases{
		"empty": {
			Input:    input{meta: "", info: ""},
			Expected: "",
		},
		"preserve from task": {
			Input:    input{meta: "meta=value"},
			Expected: "meta=value",
		},
		"append meta": {
			Input:    input{meta: "test-key=master", info: "?worker=abc"},
			Expected: "test-key=master&worker=abc",
		},
		"override meta": {
			Input:    input{meta: "key=abc&name=test", info: "?key=xyz&count=10"},
			Expected: "count=10&key=xyz&name=test",
		},
	}
	trial.New(fn, cases).SubTest(t)
}
