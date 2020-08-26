package task

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// New creates a new Task with the provided type
// and info and sets the created date.
func New(tskType, info string) *Task {
	now := time.Now().In(time.UTC)
	return &Task{
		Type:    tskType,
		Info:    info,
		ID:      uuid.New().String(),
		Created: now.Format(time.RFC3339),
		created: now,
	}
}

// NewWithID create a task with a predefined id.
// This is useful for changing tasks together
func NewWithID(tskType, info, id string) *Task {
	now := time.Now().In(time.UTC)
	return &Task{
		Type:    tskType,
		Info:    info,
		ID:      id,
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
		t.created = created.In(time.UTC)
		if t.created.IsZero() {
			t.Created = ""
		}
	}

	if t.Started != "" {
		started, err := time.Parse(time.RFC3339, t.Started)
		if err != nil {
			return nil, err
		}
		t.started = started.In(time.UTC)
		if t.started.IsZero() {
			t.Started = ""
		}
	}

	if t.Ended != "" {
		ended, err := time.Parse(time.RFC3339, t.Ended)
		if err != nil {
			return nil, err
		}
		t.ended = ended.In(time.UTC)
		if t.ended.IsZero() {
			t.Ended = ""
		}
	}

	return t, nil
}

func IsZero(t Task) bool {
	return t.Type == "" && t.Result == "" && t.Created == "" && t.Info == ""
}

type Task struct {
	Type    string `json:"type"` // identifier that indicates the type of worker that knows how to complete the task
	Info    string `json:"info"` // information that tells the worker the specifics of executing the task
	Created string `json:"created,omitempty"`
	ID      string `json:"id,omitempty"`   // unique report id
	Meta    string `json:"meta,omitempty"` // additional meta data as required

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

// Start marks the task as started
func (t *Task) Start() time.Time {
	t.started = time.Now().In(time.UTC)
	t.Started = t.started.Format(time.RFC3339)

	return t.started
}

// End will mark the task as started by populating
// Ended and setting Result.
//
func (t *Task) End(r Result, msg string) time.Time {
	t.Result = r
	t.Msg = msg
	t.ended = time.Now().In(time.UTC)
	t.Ended = t.ended.Format(time.RFC3339)

	return t.ended
}

// JSONBytes returns the task json bytes.
func (t *Task) JSONBytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	enc.Encode(t)
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// String returns json string.
func (t *Task) JSONString() string {
	return string(t.JSONBytes())
}

type Result string

const (
	CompleteResult Result = "complete" // completed successfully (as far as the worker can tell)
	ErrResult      Result = "error"    // not completed successfully (the task outcome is bad)
)
