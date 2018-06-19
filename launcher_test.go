package task

import (
	"context"
	"strings"
	"testing"
	"time"

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
	sts := string(l.Status())
	if !strings.Contains(sts, `"ActiveTasks":100`) {
		t.Errorf("Status should display ActiveTasks:100 %v", sts)
	}

	// Test Average Run time - single worker
	nop.FakeMsg = []byte(`{"type":"test","info":"?wait-time=10ms"}`)
	l = NewLauncherFromBus(newTestWorker, c, p, nil)
	l.DoTasks()
	time.Sleep(100 * time.Millisecond)
	sts = string(l.Status())
	if !strings.Contains(sts, `"AverageTaskTime":"10ms"`) {
		t.Errorf("Status should display AverageTaskTime: 10ms %v", sts)
	}

	// Test Average Run time - multiple workers
	nop.FakeMsg = []byte(`{"type":"test","info":"?wait-time=10ms"}`)
	l = NewLauncherFromBus(newTestWorker, c, p, &LauncherOptions{MaxInProgress: 20})
	l.DoTasks()
	time.Sleep(100 * time.Millisecond)
	sts = string(l.Status())
	if !strings.Contains(sts, `"AverageTaskTime":"10ms"`) {
		t.Errorf("Status should display AverageTaskTime: 10ms %v", sts)
	}

}
