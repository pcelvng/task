package task

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	// setup
	tskType := "test-type"
	tskInfo := "test-info"
	tsk := New(tskType, tskInfo)

	// correct type
	expectedType := tskType
	if tsk.Type != expectedType {
		t.Errorf("expected '%v' but got '%v'", expectedType, tsk.Type)
	}

	// correct info
	expectedInfo := tskInfo
	if tsk.Info != expectedInfo {
		t.Errorf("expected '%v' but got '%v'", expectedInfo, tsk.Info)
	}
}

func TestNewFromBytes(t *testing.T) {
	// setup - not completed task
	tskStr := `{"type":"test-type","info":"test-info","created":"2000-01-01T01:01:00Z"}`
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
	if tsk.created.IsZero() {
		t.Error("created date should not be zero")
	}

	// created date correct year
	expectedYear := 2000
	if tsk.created.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'", expectedYear, tsk.created.Year())
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
	if !tsk.started.IsZero() {
		t.Error("started date should be zero")
	}

	// ended date not populated
	if !tsk.ended.IsZero() {
		t.Error("ended date should be zero")
	}
}

func TestNewFromBytesCompleted(t *testing.T) {
	// setup - well-formed json bytes
	tskStr := `{"type":"test-type","info":"test-info","created":"2000-01-01T01:01:00Z","result":"complete","msg":"test msg","started":"2001-01-01T01:01:01Z","ended":"2002-01-01T01:01:01Z"}`
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
	if tsk.created.IsZero() {
		t.Error("created date should be populated")
	}

	// created date correct year
	expectedYear := 2000
	if tsk.created.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'\n", expectedYear, tsk.created.Year())
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
	if tsk.started.IsZero() {
		t.Error("started date should be populated")
	}

	// started date correct year
	expectedYear = 2001
	if tsk.started.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'\n", expectedYear, tsk.started.Year())
	}

	// ended date is populated
	if tsk.ended.IsZero() {
		t.Error("ended date should be populated")
	}

	// ended date correct year
	expectedYear = 2002
	if tsk.ended.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'", expectedYear, tsk.ended.Year())
	}
}

func TestNewFromBytesErr(t *testing.T) {
	// setup - wellformed json bytes
	tskStr := `{"type":"test-type","info":"test-info","created":"2000-01-01T01:01:00.01Z","result":"error","msg":"test msg","started":"2001-01-01T01:01:01Z","ended":"2002-01-01T01:01:01Z"}`
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
	if tsk.created.IsZero() {
		t.Fatal("created date should be populated")
	}

	// created date correct year
	expectedYear := 2000
	if tsk.created.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'\n", expectedYear, tsk.created.Year())
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
	if tsk.started.IsZero() {
		t.Fatalf("started date should be populated")
	}

	// started date correct year
	expectedYear = 2001
	if tsk.started.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'\n", expectedYear, tsk.started.Year())
	}

	// ended date is populated
	if tsk.ended.IsZero() {
		t.Fatal("ended date should be populated")
	}

	// ended date correct year
	expectedYear = 2002
	if tsk.ended.Year() != expectedYear {
		t.Errorf("expected '%v' but got '%v'", expectedYear, tsk.ended.Year())
	}
}

func TestTask_Bytes(t *testing.T) {
	// check bytes for correct "omitempty" fields
	tsk := &Task{
		Type: "test-type",
		Info: "test-info",
	}

	// should not have error
	b := tsk.JSONBytes()

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
	if newTsk.Created != tsk.Created {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Created, newTsk.Created)
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
	if newTsk.Started != tsk.Started {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Started, newTsk.Started)
	}

	// correct ended date
	if newTsk.Ended != tsk.Ended {
		t.Errorf("expected '%v' but got '%v'\n", tsk.Ended, newTsk.Ended)
	}

	// correctly serialized "omitempty" fields
	tsStr := "2000-01-01T00:00:00Z"
	ts, _ := time.Parse(time.RFC3339, tsStr)
	tsk.created = ts
	tsk.Created = tsStr
	tsk.Result = "test-result"
	tsk.Msg = "test msg"
	tsk.started = ts
	tsk.Started = tsStr
	tsk.ended = ts
	tsk.Ended = tsStr

	// no error
	b = tsk.JSONBytes()

	// correct byte count
	// {"type":"test-type","info":"test-info","created":"2000-01-01T00:00:00Z","result":"test-result","msg":"test msg","started":"2000-01-01T00:00:00Z","ended":"2000-01-01T00:00:00Z"}
	expectedCnt = 176
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
	if !newTsk.created.Equal(tsk.created) {
		t.Errorf("expected '%v' but got '%v'\n", tsk.created, newTsk.created)
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
	if !newTsk.started.Equal(tsk.started) {
		t.Errorf("expected '%v' but got '%v'\n", tsk.started, newTsk.started)
	}

	// correct ended date
	if !newTsk.ended.Equal(tsk.ended) {
		t.Errorf("expected '%v' but got '%v'\n", tsk.ended, newTsk.ended)
	}
}

func TestTask_String(t *testing.T) {
	// task to string plumbing test
	tsk := &Task{
		Type: "test-type",
		Info: "test-info",
	}

	tskStr := tsk.JSONString()

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
	tsk.Start()
	if tsk.started.IsZero() {
		t.Errorf("started date should be populated")
	}

	/*// correct started date - does not change with an additional call
	sameStartedAt := tsk.Start()
	if !startedAt.Equal(sameStartedAt) {
		t.Errorf("expected '%v' but got '%v'", sameStartedAt, startedAt)
	}*/
}

func TestTask_End(t *testing.T) {
	// setup
	tsk := New("test-type", "test-info")
	result := Result("test-result")
	msg := "test msg"

	// correct ended date
	tsk.End(result, msg)
	if tsk.ended.IsZero() {
		t.Fatal("expected non-zero ended date")
	}

	// correct result
	if tsk.Result != result {
		t.Errorf("expected '%v' but got '%v'\n", result, tsk.Result)
	}
	/*
		// correct ended date - should not change after another end call
		sameEnded := tsk.End(result, msg)
		if !ended.Equal(sameEnded) {
			t.Errorf("expected '%v' but got '%v'\n", sameEnded, ended)
		}

		// correct msg - should not change after another end call
		if tsk.Msg != msg {
			t.Errorf("expected '%v' but got '%v'\n", msg, tsk.Msg)
		}*/
}
