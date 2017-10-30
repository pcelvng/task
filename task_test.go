package task

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	// setup
	tskType := "test-type"
	tskInfo := "test-info"

	// func call
	tsk := New(tskType, tskInfo)

	// test that Task.Type is set correctly
	expectedType := tskType
	if tsk.Type != expectedType {
		t.Errorf("expected '%v' but got '%v'", expectedType, tsk.Type)
	}

	// test that Task.Task is set correctly
	expectedInfo := tskInfo
	if tsk.Info != expectedInfo {
		t.Errorf("expected '%v' but got '%v'", expectedInfo, tsk.Info)
	}
}

func TestNewFromBytes(t *testing.T) {
	// setup - not completed task
	tskStr := `{"type":"test-type","info":"test-info","created":"2000-01-01T01:01:00.01Z"}`
	tsk, err := NewFromBytes([]byte(tskStr))
	if err != nil {
		t.Fatalf("malformed json: '%v'", err.Error())
	}

	// correct type
	expectedType := "test-type"
	if tsk.Type != expectedType {
		t.Errorf("expected '%v' but got '%v'", expectedType, tsk.Type)
	}

	// correct info
	expectedInfo := "test-info"
	if tsk.Info != expectedInfo {
		t.Errorf("expected '%v' but got '%v'", expectedInfo, tsk.Info)
	}

	// created date populated
	if tsk.CreatedAt == nil {
		t.Error("created date should be populated")
	}

	// created date correct year
	expectedYear := 2000
	if tsk.CreatedAt.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'", expectedYear, tsk.CreatedAt.Year())
	}

	// result not populated
	expectedResult := Result("")
	if tsk.Result != expectedResult {
		t.Errorf("expected '%v' but got '%v'\n", expectedResult, tsk.Result)
	}

	// msg not populated
	expectedMsg := ""
	if tsk.Msg != expectedMsg {
		t.Errorf("expected '%v' but got '%v'\n", expectedMsg, tsk.Msg)
	}

	// started date not populated
	if tsk.StartedAt != nil {
		t.Error("started date should be nil")
	}

	// done date not populated
	if tsk.DoneAt != nil {
		t.Error("date date should be nil")
	}
}

func TestNewFromBytesCompleted(t *testing.T) {
	// setup - well-formed json bytes
	tskStr := `{"type":"test-type","info":"test-info","created":"2000-01-01T01:01:00.01Z","result":"complete","msg":"test msg","started":"2001-01-01T01:01:01Z","done":"2002-01-01T01:01:01Z"}`
	tsk, err := NewFromBytes([]byte(tskStr))
	if err != nil {
		t.Fatalf("malformed json: '%v'\n", err.Error())
	}

	// correct type
	expectedType := "test-type"
	if tsk.Type != expectedType {
		t.Errorf("expected '%v' but got '%v'\n", expectedType, tsk.Type)
	}

	// correct info
	expectedInfo := "test-info"
	if tsk.Info != expectedInfo {
		t.Errorf("expected '%v' but got '%v'\n", expectedInfo, tsk.Info)
	}

	// created date is populated
	if tsk.CreatedAt == nil {
		t.Error("created date should be populated")
	}

	// created date correct year
	expectedYear := 2000
	if tsk.CreatedAt.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'\n", expectedYear, tsk.CreatedAt.Year())
	}

	// correct result
	expectedResult := CompleteResult
	if tsk.Result != expectedResult {
		t.Errorf("expected '%v' but got '%v'\n", expectedResult, tsk.Result)
	}

	// correct msg
	expectedMsg := "test msg"
	if tsk.Msg != expectedMsg {
		t.Errorf("expected '%v' but got '%v'\n", expectedMsg, tsk.Msg)
	}

	// started date is populated
	if tsk.StartedAt == nil {
		t.Error("started date should be populated")
	}

	// started date correct year
	expectedYear = 2001
	if tsk.StartedAt.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'\n", expectedYear, tsk.StartedAt.Year())
	}

	// done date is populated
	if tsk.DoneAt == nil {
		t.Error("done date should be populated")
	}

	// done date correct year
	expectedYear = 2002
	if tsk.DoneAt.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'", expectedYear, tsk.DoneAt.Year())
	}
}

func TestNewFromBytesErr(t *testing.T) {
	// setup - wellformed json bytes
	tskStr := `{"type":"test-type","info":"test-info","created":"2000-01-01T01:01:00.01Z","result":"error","msg":"test msg","started":"2001-01-01T01:01:01Z","done":"2002-01-01T01:01:01Z"}`
	tsk, err := NewFromBytes([]byte(tskStr))
	if err != nil {
		t.Fatalf("malformed json: '%v'\n", err.Error())
	}

	// correct type
	expectedType := "test-type"
	if tsk.Type != expectedType {
		t.Errorf("expected '%v' but got '%v'\n", expectedType, tsk.Type)
	}

	// correct info
	expectedInfo := "test-info"
	if tsk.Info != expectedInfo {
		t.Errorf("expected '%v' but got '%v'\n", expectedInfo, tsk.Info)
	}

	// created date is populated
	if tsk.CreatedAt == nil {
		t.Fatal("created date should be populated")
	}

	// created date correct year
	expectedYear := 2000
	if tsk.CreatedAt.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'\n", expectedYear, tsk.CreatedAt.Year())
	}

	// correct result
	expectedResult := ErrResult
	if tsk.Result != expectedResult {
		t.Errorf("expected '%v' but got '%v'\n", expectedResult, tsk.Result)
	}

	// correct msg
	expectedMsg := "test msg"
	if tsk.Msg != expectedMsg {
		t.Errorf("expected '%v' but got '%v'\n", expectedMsg, tsk.Msg)
	}

	// started date is populated
	if tsk.StartedAt == nil {
		t.Fatalf("started date should be populated")
	}

	// started date correct year
	expectedYear = 2001
	if tsk.StartedAt.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'\n", expectedYear, tsk.StartedAt.Year())
	}

	// done date is populated
	if tsk.DoneAt == nil {
		t.Fatal("done date should be populated")
	}

	// done date correct year
	expectedYear = 2002
	if tsk.DoneAt.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'", expectedYear, tsk.DoneAt.Year())
	}
}

func TestTask_Bytes(t *testing.T) {
	// check bytes for correct "omitempty" fields
	tsk := &Task{
		Type: "test-type",
		Info: "test-info",
	}

	// should not have error
	b, err := tsk.Bytes()
	if err != nil {
		t.Fatalf("task to bytes error: '%v'\n", err.Error())
	}

	// correct byte count
	// {"type":"test-type","info":"test-info"}
	expectedCnt := 39
	if len(b) != expectedCnt {
		t.Errorf("expected '%v' but got '%v'\n", expectedCnt, len(b))
	}

	// correct new task from generated bytes
	newTsk, err := NewFromBytes(b)
	if err != nil {
		t.Fatalf("bytes to task error: '%v'\n", err.Error())
	}

	// correct type
	if newTsk.Type != tsk.Type {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Type, newTsk.Type)
	}

	// correct info
	if newTsk.Info != tsk.Info {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Info, newTsk.Info)
	}

	// correct created date
	if newTsk.CreatedAt != tsk.CreatedAt {
		t.Errorf("expected '%v' but got '%v'\n", tsk.CreatedAt, newTsk.CreatedAt)
	}

	// correct result
	if newTsk.Result != tsk.Result {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Result, newTsk.Result)
	}

	// correct msg
	if newTsk.Msg != tsk.Msg {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Msg, newTsk.Msg)
	}

	// correct started date
	if newTsk.StartedAt != tsk.StartedAt {
		t.Errorf("expected '%v' but got '%v'\n", tsk.StartedAt, newTsk.StartedAt)
	}

	// correct done date
	if newTsk.DoneAt != tsk.DoneAt {
		t.Errorf("expected '%v' but got '%v'\n", tsk.DoneAt, newTsk.DoneAt)
	}

	// correctly serialized "omitempty" fields
	ts, _ := time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
	tsk.CreatedAt = &ts
	tsk.Result = "test-result"
	tsk.Msg = "test msg"
	tsk.StartedAt = &ts
	tsk.DoneAt = &ts

	// no error
	b, err = tsk.Bytes()
	if err != nil {
		t.Fatalf("task to bytes error: '%v'\n", err.Error())
	}

	// correct byte count
	// {"type":"test-type","info":"test-info","created":"2000-01-01T00:00:00Z","result":"test-result","msg":"test msg","started":"2000-01-01T00:00:00Z","done":"2000-01-01T00:00:00Z"}
	expectedCnt = 175
	if len(b) != expectedCnt {
		t.Errorf("expected '%v' but got '%v'\n", expectedCnt, len(b))
	}

	// correct new task from generated bytes
	newTsk, err = NewFromBytes(b)
	if err != nil {
		t.Fatalf("bytes to task error: '%v'\n", err.Error())
	}

	// correct type
	if newTsk.Type != tsk.Type {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Type, newTsk.Type)
	}

	// correct info
	if newTsk.Info != tsk.Info {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Info, newTsk.Info)
	}

	// correct created date
	if !newTsk.CreatedAt.Equal(*tsk.CreatedAt) {
		t.Errorf("expected '%v' but got '%v'\n", tsk.CreatedAt, newTsk.CreatedAt)
	}

	// correct result
	if newTsk.Result != tsk.Result {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Result, newTsk.Result)
	}

	// correct msg
	if newTsk.Msg != tsk.Msg {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Msg, newTsk.Msg)
	}

	// correct started date
	if !newTsk.StartedAt.Equal(*tsk.StartedAt) {
		t.Errorf("expected '%v' but got '%v'\n", tsk.StartedAt, newTsk.StartedAt)
	}

	// correct done date
	if !newTsk.DoneAt.Equal(*tsk.DoneAt) {
		t.Errorf("expected '%v' but got '%v'\n", tsk.DoneAt, newTsk.DoneAt)
	}
}

func TestTask_String(t *testing.T) {
	// task to string plumbing test
	tsk := &Task{
		Type: "test-type",
		Info: "test-info",
	}

	tskStr, err := tsk.String()
	if err != nil {
		t.Fatalf("task to string error: '%v'\n", err.Error())
	}

	// correct byte count
	// {"type":"test-type","info":"test-info"}
	expectedCnt := 39
	if len(tskStr) != expectedCnt {
		t.Errorf("expected '%v' but got '%v'\n", expectedCnt, len(tskStr))
	}
}

func TestResult_Start(t *testing.T) {
	// setup
	tsk := New("test-type", "test-info")

	// populated started date
	startedAt := tsk.Start()
	if tsk.StartedAt == nil || tsk.StartedAt.IsZero() {
		t.Errorf("started date should be populated")
	}

	// correct started date - does not change with an additional call
	sameStartedAt := tsk.Start()
	if !startedAt.Equal(sameStartedAt) {
		t.Errorf("expected '%v' but got '%v'", sameStartedAt, startedAt)
	}
}

func TestTask_Done(t *testing.T) {
	// setup
	tsk := New("test-type", "test-info")
	result := Result("test-result")
	msg := "test msg"

	// correct done date
	doneAt := tsk.Done(result, msg)
	if tsk.DoneAt == nil || tsk.DoneAt.IsZero() {
		t.Fatal("expected non-zero done date")
	}

	// correct result
	if tsk.Result != result {
		t.Errorf("expected '%v' but got '%v'\n", result, tsk.Result)
	}

	// correct done date - should not change after another done call
	sameDoneAt := tsk.Done(result, msg)
	if !doneAt.Equal(sameDoneAt) {
		t.Errorf("expected '%v' but got '%v'\n", sameDoneAt, doneAt)
	}

	// correct msg - should not change after another done call
	if tsk.Msg != msg {
		t.Errorf("expected '%v' but got '%v'\n", msg, tsk.Msg)
	}
}
