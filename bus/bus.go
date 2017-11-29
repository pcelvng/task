package bus

type Consumer interface {
	Msg() (msg []byte, done bool, err error)
	Stop() error
}

type Producer interface {
	Send(topic string, msg []byte) error
	Stop() error
}

type Bus interface {
	Msg() (msg []byte, done bool, err error)
	Send(topic string, msg []byte) error
	Stop() error
}
