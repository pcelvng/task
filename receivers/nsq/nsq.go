package nsq

import "github.com/pcelvng/task"

type NsqReceiver struct{}

func New() (*NsqReceiver, error) {
	return &NsqReceiver{}, nil
}

func (r *NsqReceiver) Connect() error { return nil }

func (r *NsqReceiver) Next() (tsk *task.Task, last bool, err error) { return }

func (r *NsqReceiver) Done(*task.Task) {}

func (r *NsqReceiver) Close() error { return }
