package task

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
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
	c, _ := nop.NewConsumer(nop.Repeat, `{"type":"test","info":"?wait-time=5s"}`)
	p, _ := nop.NewProducer("")

	// Test MaxInProgress is 100
	l := NewLauncherFromBus(newTestWorker, c, p, &LauncherOptions{MaxInProgress: 100})
	_, cancel := l.DoTasks()

	time.Sleep(100 * time.Millisecond)
	sts := l.Stats()
	cancel()
	if sts.TasksRunning != 100 {
		t.Errorf("Tasks running should be 100 !=%d", sts.TasksRunning)
	} else {
		t.Log("PASS: 100 concurrent workers")
	}

	// Test Average Run time - single worker
	c, _ = nop.NewConsumer("", `{"type":"test","info":"?wait-time=10ms"}`)
	l = NewLauncherFromBus(newTestWorker, c, p, nil)
	l.DoTasks()
	time.Sleep(100 * time.Millisecond)
	sts = l.Stats()
	if sts.MeanTaskTime != "10ms" {
		t.Errorf("Status should display MeanTaskTime: 10ms %v", sts.MeanTaskTime)
	} else {
		t.Log("PASS: 10ms mean")
	}

	// Test Average Run time - multiple workers
	c, _ = nop.NewConsumer(nop.Repeat, `{"type":"test","info":"?wait-time=10ms"}`)
	l = NewLauncherFromBus(newTestWorker, c, p, &LauncherOptions{MaxInProgress: 20})
	l.DoTasks()
	time.Sleep(100 * time.Millisecond)
	sts = l.Stats()
	if sts.MeanTaskTime != "10ms" {
		t.Errorf("Status should display MeanTaskTime: 10ms %v", sts.MeanTaskTime)
	} else {
		t.Log("PASS: 20 concurrent workers with 10ms mean")
	}

}

// TestLauncher_Limiter verifies the rate limiter features.
// The first time a task comes in tasks up to the MaxInProgress
// should immediately start and then will be throttled by the hourly rate limit
func TestLauncher_Limiter(t *testing.T) {
	const RateLimit = 36000 // or 1 task every 100ms

	t.Run("1 concurrent worker with 100ms rate limit", func(t *testing.T) {

		//setup
		c, _ := nop.NewConsumer(nop.Repeat, `{"type":"test","info":"?wait-time=10ms"}`)
		p, _ := nop.NewProducer("")

		l := NewLauncherFromBus(newTestWorker, c, p, &LauncherOptions{MaxInProgress: 1, TaskLimit: RateLimit})
		_, cancel := l.DoTasks()
		time.Sleep(25 * time.Millisecond)
		sts := l.Stats()
		if !(sts.TasksRunning == 0 && sts.Producer.Sent["done"] == 1) {
			t.Fatal(spew.Sdump(sts))
		}

		time.Sleep(200 * time.Millisecond)
		if !(sts.TasksRunning == 0 && sts.Producer.Sent["done"] == 3 && sts.MeanTaskTime == "10ms") {
			t.Fatal(spew.Sdump(sts))
		}
		cancel()
	})

	t.Run("5 concurrent workers with 100ms rate limit", func(t *testing.T) {
		c, _ := nop.NewConsumer(nop.Repeat, `{"type":"test","info":"?wait-time=10ms"}`)
		p, _ := nop.NewProducer("")

		l := NewLauncherFromBus(newTestWorker, c, p, &LauncherOptions{MaxInProgress: 5, TaskLimit: RateLimit})
		_, cancel := l.DoTasks()
		time.Sleep(25 * time.Millisecond)
		sts := l.Stats()
		if !(sts.TasksRunning == 0 && sts.Producer.Sent["done"] == 5) {
			t.Fatal(spew.Sdump(sts))
		}

		time.Sleep(200 * time.Millisecond)
		sts = l.Stats()
		if !(sts.TasksRunning == 0 && sts.Producer.Sent["done"] == 7 && sts.MeanTaskTime == "10ms") {
			t.Fatal(spew.Sdump(sts))
		}
		cancel()
	})

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
	fn := func(in input) (string, error) {
		// Create a worker that converts all info data
		// into meta. This test is designed to test maintaining
		// the metadata across tasks without duplicating them.
		p, err := nop.NewProducer("")
		if err != nil {
			return "", err
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
			return "", err
		}
		return tsk.Meta, nil
	}
	cases := trial.Cases[input, string]{
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
