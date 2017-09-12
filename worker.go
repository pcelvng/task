package task

type Worker interface {
	DoTask(*Task) chan *Task

	// WorkerType provides the worker self
	// identifier that identifies what kind
	// of task it knows how to complete.
	WorkerType() string
	Start() error
	Close() error
}
