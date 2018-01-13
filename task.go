package task

import (
	"encoding/json"
	"time"
)

// New creates a new Task with the provided type
// and info and sets the created date.
func New(tskType, info string) *Task {
	now := time.Now()
	return &Task{
		Type:    tskType,
		Info:    info,
		Created: now.Format(time.RFC3339),
		created: now,
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

	// parse time.Time values
	// if any values come in as valid zero times then
	// modify the string to be a zero value so that
	// omitempty json serialization is correct.
	if t.Created != "" {
		created, err := time.Parse(time.RFC3339, t.Created)
		if err != nil {
			return nil, err
		}
		t.created = created
		if t.created.IsZero() {
			t.Created = ""
		}
	}

	if t.Started != "" {
		started, err := time.Parse(time.RFC3339, t.Started)
		if err != nil {
			return nil, err
		}
		t.started = started
		if t.started.IsZero() {
			t.Started = ""
		}
	}

	if t.Ended != "" {
		ended, err := time.Parse(time.RFC3339, t.Ended)
		if err != nil {
			return nil, err
		}
		t.ended = ended
		if t.ended.IsZero() {
			t.Ended = ""
		}
	}

	return t, nil
}

type Task struct {
	Type    string `json:"type"` // identifier that indicates the type of worker that knows how to complete the task
	Info    string `json:"info"` // information that tells the worker the specifics of executing the task
	Created string `json:"created,omitempty"`

	// Result fields
	Result  Result `json:"result,omitempty"`
	Msg     string `json:"msg,omitempty"`
	Started string `json:"started,omitempty"`
	Ended   string `json:"ended,omitempty"`

	// actual time.Time values so that JSON is omitempty correct and time.Time
	// values are not pointers.
	created time.Time
	started time.Time
	ended   time.Time
}

// Start will mark the task as started by populating
// Started.
//
// If Started is already populated then the existing
// value is returned.
func (t *Task) Start() time.Time {
	if t.started.IsZero() {
		t.started = time.Now()
		t.Started = t.started.Format(time.RFC3339)
	}

	return t.started
}

// End will mark the task as started by populating
// Ended and setting Result.
//
// If Ended is already populated then the existing
// value is returned.
func (t *Task) End(r Result, msg string) time.Time {
	if t.ended.IsZero() {
		t.Result = r
		t.Msg = msg
		t.ended = time.Now()
		t.Ended = t.started.Format(time.RFC3339)
	}

	return t.ended
}

// JSONBytes returns the task json bytes.
func (t *Task) JSONBytes() ([]byte, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// String returns json string.
func (t *Task) JSONString() (string, error) {
	b, err := t.JSONBytes()
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
