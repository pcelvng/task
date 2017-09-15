package task

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	// setup
	taskType := "type.test"
	taskTask := "test_task"

	// func call
	ttask := New(taskType, taskTask)

	// test that Task.Type is set correctly
	expectedType := taskType
	if ttask.Type != expectedType {
		t.Errorf("expected '%v' but got '%v'", expectedType, ttask.Type)
	}

	// test that Task.Task is set correctly
	expectedTask := taskTask
	if ttask.Task != expectedTask {
		t.Errorf("expected '%v' but got '%v'", expectedTask, ttask.Task)
	}
}

func TestNewFromBytes(t *testing.T) {
	// setup - not completed task
	tstring := `{"type":"type.test","task":"test_task","timestamp":"2000-01-01T01:01:00.01Z"}`
	tbytes := []byte(tstring)

	ttask, err := NewFromBytes(tbytes)
	if err != nil {
		t.Fatalf("malformed json: '%v'", err.Error())
	}

	// Check Task.Type
	expectedType := "type.test"
	if ttask.Type != expectedType {
		t.Errorf("expected '%v' but got '%v'", expectedType, ttask.Type)
	}

	// Check Task.Type
	expectedTask := "test_task"
	if ttask.Task != expectedTask {
		t.Errorf("expected '%v' but got '%v'", expectedTask, ttask.Task)
	}

	// Check Task.Timestamp is populated
	if ttask.Timestamp == nil || ttask.Timestamp.IsZero() {
		t.Errorf("task.Timestamp should be populated")
	}

	// Check Task.Timestamp has correct year
	expectedTimestampYear := 2000
	if ttask.Timestamp.Year() != expectedTimestampYear {
		t.Errorf("expected '%v' but got '%v'", expectedTimestampYear, ttask.Timestamp.Year())
	}

	// Check Task.Result
	expectedResult := Result("")
	if ttask.Result != expectedResult {
		t.Errorf("expected '%v' but got '%v'", expectedResult, ttask.Result)
	}

	// Check Task.Msg
	expectedMsg := ""
	if ttask.Msg != expectedMsg {
		t.Errorf("expected '%v' but got '%v'", expectedMsg, ttask.Msg)
	}

	// Check Task.Started is zero
	if ttask.Started != nil && !ttask.Started.IsZero() {
		t.Errorf("task.Started should not be populated")
	}

	// Check Task.Completed is populated
	if ttask.Completed != nil && !ttask.Completed.IsZero() {
		t.Errorf("task.Completed should not be populated")
	}
}

func TestNewFromBytesCompleted(t *testing.T) {
	// setup - wellformed json bytes
	tstring := `{"type":"type.test","task":"test_task","timestamp":"2000-01-01T01:01:00.01Z","result":"complete","started":"2001-01-01T01:01:01Z","completed":"2002-01-01T01:01:01Z"}`
	tbytes := []byte(tstring)

	ttask, err := NewFromBytes(tbytes)
	if err != nil {
		t.Fatalf("malformed json: '%v'", err.Error())
	}

	// Check Task.Type
	expectedType := "type.test"
	if ttask.Type != expectedType {
		t.Errorf("expected '%v' but got '%v'", expectedType, ttask.Type)
	}

	// Check Task.Type
	expectedTask := "test_task"
	if ttask.Task != expectedTask {
		t.Errorf("expected '%v' but got '%v'", expectedTask, ttask.Task)
	}

	// Check Task.Timestamp is populated
	if ttask.Timestamp == nil || ttask.Timestamp.IsZero() {
		t.Errorf("task.Timestamp should be populated")
	}

	// Check Task.Timestamp has correct year
	expectedTimestampYear := 2000
	if ttask.Timestamp.Year() != expectedTimestampYear {
		t.Errorf("expected '%v' but got '%v'", expectedTimestampYear, ttask.Timestamp.Year())
	}

	// Check Task.Result
	expectedResult := CompleteResult
	if ttask.Result != expectedResult {
		t.Errorf("expected '%v' but got '%v'", expectedResult, ttask.Result)
	}

	// Check Task.Msg
	expectedMsg := ""
	if ttask.Msg != expectedMsg {
		t.Errorf("expected '%v' but got '%v'", expectedMsg, ttask.Msg)
	}

	// Check Task.Started is populated
	if ttask.Started == nil || ttask.Started.IsZero() {
		t.Errorf("task.Started should be populated")
	}

	// Check Task.Started has correct year
	expectedStartedYear := 2001
	if ttask.Started.Year() != expectedStartedYear {
		t.Errorf("expected '%v' but got '%v'", expectedStartedYear, ttask.Started.Year())
	}

	// Check Task.Completed is populated
	if ttask.Completed == nil || ttask.Completed.IsZero() {
		t.Errorf("task.Completed should be populated")
	}

	// Check Task.Completed has correct year
	expectedCompletedYear := 2002
	if ttask.Completed.Year() != expectedCompletedYear {
		t.Errorf("expected '%v' but got '%v'", expectedCompletedYear, ttask.Completed.Year())
	}
}

func TestNewFromBytesErr(t *testing.T) {
	// setup - wellformed json bytes
	tstring := `{"type":"type.test","task":"test_task","timestamp":"2000-01-01T01:01:00.01Z","result":"error","msg":"test msg","started":"2001-01-01T01:01:01Z","completed":"2002-01-01T01:01:01Z"}`
	tbytes := []byte(tstring)

	ttask, err := NewFromBytes(tbytes)
	if err != nil {
		t.Fatalf("malformed json: '%v'", err.Error())
	}

	// Check Task.Type
	expectedType := "type.test"
	if ttask.Type != expectedType {
		t.Errorf("expected '%v' but got '%v'", expectedType, ttask.Type)
	}

	// Check Task.Type
	expectedTask := "test_task"
	if ttask.Task != expectedTask {
		t.Errorf("expected '%v' but got '%v'", expectedTask, ttask.Task)
	}

	// Check Task.Timestamp is populated
	if ttask.Timestamp == nil || ttask.Timestamp.IsZero() {
		t.Errorf("task.Timestamp should be populated")
	}

	// Check Task.Timestamp has correct year
	expectedTimestampYear := 2000
	if ttask.Timestamp.Year() != expectedTimestampYear {
		t.Errorf("expected '%v' but got '%v'", expectedTimestampYear, ttask.Timestamp.Year())
	}

	// Check Task.Result
	expectedResult := ErrResult
	if ttask.Result != expectedResult {
		t.Errorf("expected '%v' but got '%v'", expectedResult, ttask.Result)
	}

	// Check Task.Msg
	expectedMsg := "test msg"
	if ttask.Msg != expectedMsg {
		t.Errorf("expected '%v' but got '%v'", expectedMsg, ttask.Msg)
	}

	// Check Task.Started is populated
	if ttask.Started == nil || ttask.Started.IsZero() {
		t.Errorf("task.Started should be populated")
	}

	// Check Task.Started has correct year
	expectedStartedYear := 2001
	if ttask.Started.Year() != expectedStartedYear {
		t.Errorf("expected '%v' but got '%v'", expectedStartedYear, ttask.Started.Year())
	}

	// Check Task.Completed is populated
	if ttask.Completed == nil || ttask.Completed.IsZero() {
		t.Errorf("task.Completed should be populated")
	}

	// Check Task.Completed has correct year
	expectedCompletedYear := 2002
	if ttask.Completed.Year() != expectedCompletedYear {
		t.Errorf("expected '%v' but got '%v'", expectedCompletedYear, ttask.Completed.Year())
	}
}

func TestTask_Valid(t *testing.T) {
	// setup
	ttask := &Task{}
	now := time.Now()

	// empty task is not valid
	if ttask.Valid("") == nil {
		t.Errorf("empty task should not be valid")
	}

	// just Task.Type is not valid
	ttask.Type = "testtype"
	if ttask.Valid("") == nil {
		t.Errorf("valid task should require at least a type, task and timestamp")
	}

	// just Task.Task is not valid
	ttask.Type = ""
	ttask.Task = "testtask"
	if ttask.Valid("") == nil {
		t.Errorf("valid task should require at least a type, task and timestamp")
	}

	// just Task.Timestamp is not valid
	ttask.Type = ""
	ttask.Task = ""
	ttask.Timestamp = &now
	if ttask.Valid("") == nil {
		t.Errorf("valid task should require at least a type, task and timestamp")
	}

	// including type, task and timestamp is valid
	ttask.Type = "testtype"
	ttask.Task = "testtask"
	ttask.Timestamp = &now
	if ttask.Valid("") != nil {
		t.Errorf("task should be valid")
	}

	// if task type is provided then the provided task type
	// should match the assigned task type.
	if ttask.Valid("testtype") != nil {
		t.Errorf("task should be valid")
	}

	// if task type is provided then the provided task type
	// should match the assigned task type.
	if ttask.Valid("different_testtype") == nil {
		t.Errorf("task should not be valid")
	}

	// empty result should not have Completed date
	ttask.Completed = &now
	if ttask.Valid("") == nil {
		t.Errorf("task should not be valid")
	}

	// empty result should have empty error msg
	ttask.Completed = nil
	ttask.Msg = "notempty"
	if ttask.Valid("") == nil {
		t.Errorf("task should not be valid")
	}

	// result value should be valid
	ttask.Result = "invalidresult"
	if ttask.Valid("") == nil {
		t.Errorf("task should not be valid")
	}

	// result value should be valid
	ttask.Result = CompleteResult
	ttask.Started = &now
	ttask.Completed = &now
	if ttask.Valid("") != nil {
		t.Errorf("task should be valid")
	}

	// result value should be valid
	ttask.Result = ErrResult
	ttask.Started = &now
	ttask.Completed = &now
	ttask.Msg = "testmsg"
	if ttask.Valid("") != nil {
		t.Errorf("task should be valid")
	}

	// error result should have a message
	ttask.Result = ErrResult
	ttask.Started = &now
	ttask.Completed = &now
	ttask.Msg = ""
	if ttask.Valid("") == nil {
		t.Errorf("task should not be valid")
	}
}

func TestInvalidError_Error(t *testing.T) {
	// setup
	errMsg := "error message"
	err := &InvalidError{msg: errMsg}
	if err.Error() != errMsg {
		t.Errorf("expected '%v' but got '%v'", errMsg, err.Error())
	}
}

func TestTask_Bytes(t *testing.T) {
	// check bytes of task without "omitempty" type fields
	ttask := &Task{
		Type: "test_type",
		Task: "test_task",
	}

	// check for error
	b, err := ttask.Bytes()
	if err != nil {
		t.Fatalf("task to bytes error: '%v'", err.Error())
	}

	// check byte count
	// {"type":"test_type","task":"test_task"}
	expectedByteCount := 39
	if len(b) != expectedByteCount {
		t.Errorf("expected '%v' but got '%v'", expectedByteCount, len(b))
	}

	// check that bytes result becomes the same task
	newTask, err := NewFromBytes(b)
	if err != nil {
		t.Fatalf("bytes to task error: '%v'", err.Error())
	}

	// check Type
	if newTask.Type != ttask.Type {
		t.Errorf("expected '%v' but got '%v'", ttask.Type, newTask.Type)
	}

	// check Task
	if newTask.Task != ttask.Task {
		t.Errorf("expected '%v' but got '%v'", ttask.Task, newTask.Task)
	}

	// check Timestamp
	if newTask.Timestamp != ttask.Timestamp {
		t.Errorf("expected '%v' but got '%v'", ttask.Timestamp, newTask.Timestamp)
	}

	// check Result
	if newTask.Result != ttask.Result {
		t.Errorf("expected '%v' but got '%v'", ttask.Result, newTask.Result)
	}

	// check Msg
	if newTask.Msg != ttask.Msg {
		t.Errorf("expected '%v' but got '%v'", ttask.Msg, newTask.Msg)
	}

	// check Started
	if newTask.Started != ttask.Started {
		t.Errorf("expected '%v' but got '%v'", ttask.Started, newTask.Started)
	}

	// check Completed
	if newTask.Completed != ttask.Completed {
		t.Errorf("expected '%v' but got '%v'", ttask.Completed, newTask.Completed)
	}

	// check bytes of "omitempty" type members
	ts, _ := time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
	ttask.Timestamp = &ts
	ttask.Result = "test_result"
	ttask.Msg = "test message"
	ttask.Started = &ts
	ttask.Completed = &ts

	// check for error
	b, err = ttask.Bytes()
	if err != nil {
		t.Fatalf("task to bytes error: '%v'", err.Error())
	}

	// check byte count
	// {"type":"test_type","task":"test_task","timestamp":"2000-01-01T00:00:00Z","result":"test_result","msg":"test message","started":"2000-01-01T00:00:00Z","completed":"2000-01-01T00:00:00Z"}
	expectedByteCount = 186
	if len(b) != expectedByteCount {
		t.Errorf("expected '%v' but got '%v'", expectedByteCount, len(b))
	}

	// convert bytes back to Task and check fields
	// check that bytes result becomes the same task
	newTask, err = NewFromBytes(b)
	if err != nil {
		t.Fatalf("bytes to task error: '%v'", err.Error())
	}

	// check Type
	if newTask.Type != ttask.Type {
		t.Errorf("expected '%v' but got '%v'", ttask.Type, newTask.Type)
	}

	// check Task
	if newTask.Task != ttask.Task {
		t.Errorf("expected '%v' but got '%v'", ttask.Task, newTask.Task)
	}

	// check Timestamp
	if !newTask.Timestamp.Equal(*ttask.Timestamp) {
		t.Errorf("expected '%v' but got '%v'", ttask.Timestamp, newTask.Timestamp)
	}

	// check Result
	if newTask.Result != ttask.Result {
		t.Errorf("expected '%v' but got '%v'", ttask.Result, newTask.Result)
	}

	// check Msg
	if newTask.Msg != ttask.Msg {
		t.Errorf("expected '%v' but got '%v'", ttask.Msg, newTask.Msg)
	}

	// check Started
	if !newTask.Started.Equal(*ttask.Started) {
		t.Errorf("expected '%v' but got '%v'", ttask.Started, newTask.Started)
	}

	// check Completed
	if !newTask.Completed.Equal(*ttask.Completed) {
		t.Errorf("expected '%v' but got '%v'", ttask.Completed, newTask.Completed)
	}
}

func TestTask_String(t *testing.T) {
	// check Task.String plumbing
	ttask := &Task{
		Type: "test_type",
		Task: "test_task",
	}

	tstring, err := ttask.String()
	if err != nil {
		t.Fatalf("task to string error: '%v'", err.Error())
	}

	// Check that matches the correct length
	// {"type":"test_type","task":"test_task"}
	expectedLength := 39
	if len(tstring) != expectedLength {
		t.Errorf("expected '%v' but got '%v'", expectedLength, len(tstring))
	}
}

func TestResult_Start(t *testing.T) {
	// setup
	ttask := New("test_type", "test_task")

	// test that the Task is started
	startedAt := ttask.Start()
	if ttask.Started == nil || ttask.Started.IsZero() {
		t.Errorf("task should have been started")
	}

	// check that the Started value doesn't change with another call
	sameStartedAt := ttask.Start()
	if !startedAt.Equal(sameStartedAt) {
		t.Errorf("expected '%v' but got '%v'", sameStartedAt, startedAt)
	}
}

func TestResult_InProgress(t *testing.T) {
	// setup
	ttask := New("test_type", "test_task")

	// should not be in progress since it hasn't started yet
	if ttask.InProgress() {
		t.Error("task should not be marked as in progress")
	}

	// start the task - should now be in progress
	ttask.Start()
	if !ttask.InProgress() {
		t.Error("task should be marked as in progress")
	}

	// complete the task - should no longer be in progress
	msg := ""
	ttask.Complete(msg)
	if ttask.InProgress() {
		t.Error("task should not be marked as in progress")
	}
}

func TestResult_InProgressErr(t *testing.T) {
	// setup
	ttask := New("test_type", "test_task")

	// should not be in progress since it hasn't started yet
	if ttask.InProgress() {
		t.Error("task should not be marked as in progress")
	}

	// start the task - should now be in progress
	ttask.Start()
	if !ttask.InProgress() {
		t.Error("task should be marked as in progress")
	}

	// complete the task - should no longer be in progress
	ttask.Err("not completed")
	if ttask.InProgress() {
		t.Error("task should not be marked as in progress")
	}
}

func TestResult_Complete(t *testing.T) {
	// setup
	ttask := New("test_type", "test_task")

	// test that the Task is completed
	msg := ""
	completedAt := ttask.Complete(msg)
	if ttask.Completed == nil || ttask.Completed.IsZero() {
		t.Errorf("task should have a non-zero completed value")
	}

	// should have complete result status
	if ttask.Result != CompleteResult {
		t.Errorf("expected '%v' but got '%v'", CompleteResult, ttask.Result)
	}

	// check that the Completed value doesn't change with another call
	sameCompletedAt := ttask.Complete(msg)
	if !completedAt.Equal(sameCompletedAt) {
		t.Errorf("expected '%v' but got '%v'", sameCompletedAt, completedAt)
	}

	// an additional call to Err() should not change anything
	sameCompletedAt = ttask.Err("some message")

	// should have the same original completed timestamp
	if !completedAt.Equal(sameCompletedAt) {
		t.Errorf("expected '%v' but got '%v'", sameCompletedAt, completedAt)
	}

	// should have complete result status still
	if ttask.Result != CompleteResult {
		t.Errorf("expected '%v' but got '%v'", CompleteResult, ttask.Result)
	}

	// message should not have changed
	if ttask.Msg != "" {
		t.Errorf("expected '%v' but got '%v'", "", ttask.Msg)
	}

	// check that the Complete msg is set correctly
	// and doesn't change with subsequent calls.
	ttask = New("test_type", "test_task")
	msg = "test msg"
	ttask.Complete(msg)
	if ttask.Msg != msg {
		t.Errorf("expected '%v' but got '%v'", msg, ttask.Msg)
	}

	// test subsequent call does not change the msg.
	msg = "new test msg"
	ttask.Complete(msg)
	if ttask.Msg == msg {
		t.Errorf("expected '%v' but got '%v'", msg, ttask.Msg)
	}

}

func TestResult_IsCompleted(t *testing.T) {
	// setup
	ttask := New("test_type", "test_task")

	// should not be in progress since it hasn't started yet
	if ttask.IsCompleted() {
		t.Error("task should not be marked as in progress")
	}

	// start the task - should still not be completed
	ttask.Start()
	if ttask.IsCompleted() {
		t.Error("task should not be marked as in progress")
	}

	// complete the task - should be completed now
	msg := ""
	ttask.Complete(msg)
	if !ttask.IsCompleted() {
		t.Error("task should be marked as completed")
	}

	// should not be marked as error
	if ttask.IsErr() {
		t.Error("task should not be marked as error")
	}

	ttask.Err("error message")
	// should still not be marked as error
	if ttask.IsErr() {
		t.Error("task should not be marked as error")
	}

}

func TestResult_IsErr(t *testing.T) {
	// setup
	ttask := New("test_type", "test_task")

	// should not be in progress since it hasn't started yet
	if ttask.IsErr() {
		t.Error("task should not be marked as error")
	}

	// start the task - should still not be completed
	ttask.Start()
	if ttask.IsErr() {
		t.Error("task should not be marked as error")
	}

	// complete the task - should be completed now
	ttask.Err("error message")
	if !ttask.IsErr() {
		t.Error("task should be marked as error")
	}

	// should not be marked as completed
	if ttask.IsCompleted() {
		t.Error("task should not be marked as completed")
	}

	msg := ""
	ttask.Complete(msg)
	// should still not be marked as completed
	if ttask.IsCompleted() {
		t.Error("task should not be marked as completed")
	}
}

func TestResult_Err(t *testing.T) {
	// setup
	ttask := New("test_type", "test_task")

	// test that the Task is put in an error state
	completedAt := ttask.Err("test message")
	if ttask.Completed == nil || ttask.Completed.IsZero() {
		t.Errorf("task should have a non-zero completed value")
	}

	// should have error result status
	if ttask.Result != ErrResult {
		t.Errorf("expected '%v' but got '%v'", ErrResult, ttask.Result)
	}

	// should have error message
	expectedMsg := "test message"
	if ttask.Msg != expectedMsg {
		t.Errorf("expected '%v' but got '%v'", expectedMsg, ttask.Msg)
	}

	// check that the Completed value doesn't change with another call
	sameCompletedAt := ttask.Err("new error message")
	if !completedAt.Equal(sameCompletedAt) {
		t.Errorf("expected '%v' but got '%v'", sameCompletedAt, completedAt)
	}

	// should still have error result status
	if ttask.Result != ErrResult {
		t.Errorf("expected '%v' but got '%v'", ErrResult, ttask.Result)
	}

	// should have original error message
	if ttask.Msg != expectedMsg {
		t.Errorf("expected '%v' but got '%v'", expectedMsg, ttask.Msg)
	}

	// calling Complete() should not change anything
	msg := ""
	ttask.Complete(msg)

	// should still be the same original datetime
	if !completedAt.Equal(sameCompletedAt) {
		t.Errorf("expected '%v' but got '%v'", sameCompletedAt, completedAt)
	}

	// should still have error result status
	if ttask.Result != ErrResult {
		t.Errorf("expected '%v' but got '%v'", ErrResult, ttask.Result)
	}

	// should have original error message
	if ttask.Msg != expectedMsg {
		t.Errorf("expected '%v' but got '%v'", expectedMsg, ttask.Msg)
	}

}

func TestResult_Valid(t *testing.T) {
	// ErrResult constant is valid
	if !ErrResult.Valid() {
		t.Errorf("'Result' of constant of value '%v' should be valid", ErrResult)
	}

	// 'Result' from ErrResult constant is valid
	errResult := ErrResult
	if !errResult.Valid() {
		t.Errorf("'Result' of constant of value '%v' should be valid", errResult)
	}

	// 'Result' from CompleteResult constant is valid
	completedResult := CompleteResult
	if !completedResult.Valid() {
		t.Errorf("'Result' of constant of value '%v' should be valid", completedResult)
	}

	// Empty result is valid
	emptyResult := Result("")
	if !emptyResult.Valid() {
		t.Errorf("'Result' of value '%v' should be valid", emptyResult)
	}

	// "error" result from string is valid
	errResult = Result("error")
	if !errResult.Valid() {
		t.Errorf("'Result' of value '%v' should be valid", errResult)
	}

	// Sanity check: "" (empty) task result is valid
	ttask := &Task{}
	if !ttask.Result.Valid() {
		t.Errorf("'error' 'Task.Result' with value '%v' should be valid", ttask.Result)
	}

	// Sanity check: "error" task result is valid
	ttask.Result = "error"
	if !ttask.Result.Valid() {
		t.Errorf("'error' 'Task.Result' with value '%v' should be valid", ttask.Result)
	}

	// Sanity check: "complete" task result is valid
	ttask.Result = "complete"
	if !ttask.Result.Valid() {
		t.Errorf("'error' 'Task.Result' with value '%v' should be valid", ttask.Result)
	}

	// Test invalid Result
	invalidResult := Result("invalid")
	if invalidResult.Valid() {
		t.Errorf("'error' 'Result' with value '%v' should be invalid", invalidResult)
	}

	// Sanity check: Test invalid Result from Task.Result
	ttask.Result = Result("invalid")
	if ttask.Result.Valid() {
		t.Errorf("'error' 'Result' with value '%v' should be invalid", ttask.Result)
	}
}
