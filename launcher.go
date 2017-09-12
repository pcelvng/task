package task

type Launcher interface {
	Do()
	Start() error
	Close() error
}
