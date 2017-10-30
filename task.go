package task

import (
	"encoding/json"
	"time"
)

// New creates a new Task with the provided type
// and info and sets the created date.
func New(tskType, info string) *Task {
	now := time.Now().Round(time.Second)
	return &Task{
		Type:      tskType,
		Info:      info,
		CreatedAt: &now,
	}
}

// NewFromBytes creates a Task from
// json bytes.
func NewFromBytes(b []byte) (*Task, error) {
	t := &Task{}
	err := json.Unmarshal(b, t)
	if err != nil {
		return nil, err
	}

	return t, nil
}

type Task struct {
	Type      string     `json:"type"` // identifier that indicates the type of worker that knows how to complete the task
	Info      string     `json:"info"` // information that tells the worker the specifics of executing the task
	CreatedAt *time.Time `json:"created,omitempty"`

	// Result fields
	Result    Result     `json:"result,omitempty"`
	Msg       string     `json:"msg,omitempty"`
	StartedAt *time.Time `json:"started,omitempty"`
	DoneAt    *time.Time `json:"done,omitempty"`
}

// Start will mark the task as started by populating
// StartedAt.
//
// If StartedAt is already populated then the existing
// value is returned.
func (t *Task) Start() time.Time {
	if t.StartedAt == nil || t.StartedAt.IsZero() {
		now := time.Now().Round(time.Second)
		t.StartedAt = &now
	}

	return *t.StartedAt
}

// Done will mark the task as started by populating
// DoneAt and setting Result.
//
// If DoneAt is already populated then the existing
// value is returned.
func (t *Task) Done(r Result, msg string) time.Time {
	if t.DoneAt == nil || t.DoneAt.IsZero() {
		t.Result = r
		t.Msg = msg
		now := time.Now().Round(time.Second)
		t.DoneAt = &now
	}

	return *t.DoneAt
}

// Bytes returns the json bytes.
func (t *Task) Bytes() ([]byte, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// String returns json string.
func (t *Task) String() (string, error) {
	b, err := t.Bytes()
	if err != nil {
		return "", err
	}

	return string(b), nil
}

type Result string

const (
	CompleteResult Result = "complete" // completed successfully (as far as the worker can tell)
	ErrResult      Result = "error"    // not completed successfully (the task outcome is bad)
)
