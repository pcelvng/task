// Package simple implements the receiver interface.
package simple

import (
	"bufio"
	"log"
	"os"
	"strings"

	"github.com/pcelvng/task"
)

type SimpleReceiver struct {
	tsks []*task.Task
}

func New(tsks []*task.Task) (*SimpleReceiver, error) {
	// should provide at least one task
	if len(tsks) == 0 {
		return nil, &task.NoTasksError{}
	}

	return &SimpleReceiver{
		tsks: tsks,
	}, nil
}

func NewFromFile(path string) (*SimpleReceiver, error) {
	// attempt to read from the file path
	// and create tasks.
	tsks, err := fetchTasks(path)
	if err != nil {
		return nil, err
	}

	// should provide at least one task
	if len(tsks) == 0 {
		return nil, &task.NoTasksError{}
	}

	return &SimpleReceiver{
		tsks: tsks,
	}, nil
}

// fetchTasks will attempt to read from the file and
// parse each line as a Task. If there are any problems
// found then no Tasks are return and the error is reported.
func fetchTasks(path string) ([]*task.Task, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	tsks := make([]*task.Task, 0, 100)
	var tsk *task.Task
	for {
		if scanner.Scan() {
			tsk, err = task.NewFromBytes(scanner.Bytes())
			if err != nil {
				return nil, err
			}
			tsks = append(tsks, tsk)
		} else {
			// special treatment for last line
			b := scanner.Bytes()

			// check that the last line has bytes and is probably json.
			if len(b) > 0 && strings.Contains(string(b), "{") {
				// attempt to parse
				tsk, err = task.NewFromBytes(b)
				if err != nil {
					return nil, err
				}
				tsks = append(tsks, tsk)
			}

			break
		}
	}

	return tsks, nil
}

func (r *SimpleReceiver) Next() (next *task.Task, last bool, err error) {
	if r.tsks == nil || len(r.tsks) == 0 {
		return next, last, &task.NoTasksError{}
	}

	if len(r.tsks) == 1 {
		last = true
	}

	// provide the first task of the list
	// and update the list.
	next, r.tsks = r.tsks[0], r.tsks[1:]

	return next, last, err
}

// Done will log the task to stderr.
func (r *SimpleReceiver) Done(t *task.Task) {
	b, err := t.Bytes()
	if err != nil {
		log.Printf("unable to convert task to bytes: '%v'", err.Error())
	}

	log.Print(string(b))
}

// Connect doesn't apply in this case.
func (r *SimpleReceiver) Connect() error {
	return nil
}

// Close doesn't apply in this case.
func (r *SimpleReceiver) Close() error {
	return nil
}
