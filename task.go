package task

import (
	"encoding/json"
	"fmt"
	"time"
)

// New expects a task type, task. The timestamp
// is set upon creation.
func New(ttype, task string) *Task {
	now := time.Now()
	return &Task{
		Type:    ttype,
		Task:    task,
		Created: &now,
	}
}

// NewFromBytes expects a byte array that represents a
// valid json task. If the byte array is malformed JSON
// then *Task will return nil and error will not be null.
func NewFromBytes(b []byte) (*Task, error) {
	t := &Task{}
	err := json.Unmarshal(b, t)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// Task contains all the officially supported Task members.
//
// Make sure to start and complete a task using the Start() and
// Complete() methods. If there is a known problem completing a
// task then call the Err() method. Do not set the Task members
// directly.
type Task struct {
	// Core Task parameters
	Type    string     `json:"type"` // "audit.fb.api"
	Task    string     `json:"task"`
	Created *time.Time `json:"created,omitempty"`

	// Task result parameters
	Result    Result     `json:"result,omitempty"`
	Msg       string     `json:"msg,omitempty"`
	Started   *time.Time `json:"started,omitempty"`
	Completed *time.Time `json:"completed,omitempty"`
}

// Bytes will return the json bytes from the Task
func (t *Task) Bytes() ([]byte, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// String will return the json bytes from the Task.
// If there is an error Marshaling the JSON then the
// Marshal error will be returned and string will be empty.
func (t *Task) String() (string, error) {
	b, err := t.Bytes()
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// Start will mark the task as started by adding a started time.
// Start should not be called more than once but doing so is safe. It will
// not mark a new Started time.Time but will keep the existing Started value
// and return that value.
func (t *Task) Start() time.Time {
	if t.Started == nil || t.Started.IsZero() {
		now := time.Now()
		t.Started = &now
	}

	return *t.Started
}

// InProgress will check if the task is considered in progress
// where 'in progress' is a task that has been started but not completed
func (t *Task) InProgress() bool {
	if t.Started == nil || t.Started.IsZero() {
		// task is not even started
		return false
	}

	if t.Completed == nil || t.Completed.IsZero() {
		// task is started and not completed
		return true
	}

	// task is started and completed so it is not
	// in progress
	return false
}

// Complete will mark the task as completed by setting the result
// as completed successfully and set the Completed time.Time value.
// Calling Complete more than once during a task lifecycle is bad practice
// but it is safe to do so. Doing so will not update the Completed timestamp on
// the Task.
//
// Unlike Err, the Complete 'msg' is optional.
//
// It is bad practice to call both the Complete() and Err() Task methods.
func (t *Task) Complete(msg string) time.Time {
	if t.Completed == nil || t.Completed.IsZero() {
		t.Result = CompleteResult
		t.Msg = msg
		now := time.Now()
		t.Completed = &now
	}

	return *t.Completed
}

// IsComplete will check if the task is marked as completed.
// A 'completed task' is a task with a non-zero completed date.
//
// If a Task has a zero Started date and a non-zero completed date
// it is still considered completed but invalid. A call to Task.Valid()
// will yield a validation error.
func (t *Task) IsCompleted() bool {
	if t.Completed == nil || t.Completed.IsZero() {
		return false
	}

	if t.Result == CompleteResult {
		return true
	}

	return false
}

// Err will mark the Task Completed time value and as add the Task message with the value of 'msg'.
// Calling Err is bad practice and only the first call will stick. If called more than once only
// the original timestamp is returned.
//
// It is bad practice to call both the Complete() and Err() Task methods.
func (t *Task) Err(msg string) time.Time {
	if t.Completed == nil || t.Completed.IsZero() {
		t.Result = ErrResult
		now := time.Now()
		t.Completed = &now
		t.Msg = msg
	}

	return *t.Completed
}

// IsErr will return true if the task was marked
// with a processing error.
func (t *Task) IsErr() bool {
	if t.Completed == nil || t.Completed.IsZero() {
		return false
	}

	if t.Result == ErrResult {
		return true
	}

	return false
}

// Valid will check that the task is in a valid state.
// A valid state is one where the task has valid field
// combinations. A valid task will return nil.
//
// Valid Task state assertions:
// - A Task should have a type, task, and non-zero timestamp
// - If a Task type is supplied to the method then the task type should match the
//   provided Task type
// - A completed Task should have a valid result
// - A Task with a error result should have an error message
// - A Task with a result should have non-zero started and completed dates
// - A Task without a result should have a zero value completed date and error message
func (t *Task) Valid(taskType string) error {
	errMsg := ""

	// Has Task.Type
	if t.Type == "" {
		errMsg = "task does not have a type value"
		return &InvalidError{msg: errMsg}
	}

	// Task.Type matches desired task type
	if taskType != "" && (t.Type != taskType) {
		errMsg = fmt.Sprintf("task type is '%v' but needs to be '%v'", t.Type, taskType)
		return &InvalidError{msg: errMsg}
	}

	// Has Task.Task
	if t.Task == "" {
		errMsg = "task does not have a task value"
		return &InvalidError{msg: errMsg}
	}

	// Has non-zero Task.Created
	if t.Created == nil || t.Created.IsZero() {
		errMsg = "task timestamp is a zero value"
		return &InvalidError{msg: errMsg}
	}

	// Empty result validation
	if t.Result == Result("") {
		// Have zero-value completed
		if t.Completed != nil && !t.Completed.IsZero() {
			errMsg = "task has no result so should not have a completed date"
			return &InvalidError{msg: errMsg}
		}

		// Task.Msg should be be empty
		if t.Msg != "" {
			errMsg = "task has no result so should not have an error message"
			return &InvalidError{msg: errMsg}
		}
	}

	// Non-empty result validation
	if t.Result != Result("") {
		// Valid value
		if !t.Result.Valid() {
			errMsg = fmt.Sprintf("invalid task result value '%v'", t.Result)
			return &InvalidError{msg: errMsg}
		}

		// Has non-zero Task.Started
		if t.Started == nil || t.Started.IsZero() {
			errMsg = "task has a result but was not started"
			return &InvalidError{msg: errMsg}
		}

		// Has non-zero Task.Completed
		if t.Completed == nil || t.Completed.IsZero() {
			errMsg = "task has a result but was not completed"
			return &InvalidError{msg: errMsg}
		}
	}

	// Error Result should also have Task.Msg
	if t.Result == ErrResult && t.Msg == "" {
		errMsg = "task has a error result but no error message"
		return &InvalidError{msg: errMsg}
	}

	return nil
}

type InvalidError struct {
	msg string
}

func (e *InvalidError) Error() string {
	return e.msg
}

// Result string type
type Result string

// Valid will check that Result is one
// of the standard result values.
func (t Result) Valid() bool {
	switch t {
	case CompleteResult, ErrResult:
		return true
	case Result(""):
		return true
	}

	return false
}

const (
	CompleteResult Result = "complete" // completed successfully as far as the worker can tell
	ErrResult      Result = "error"    // the task was unable to be completed successfully

)
