package bus

type Consumer interface {
	Connect() error
	Msg() ([]byte, error)
	Close() error
}

type Producer interface {
	Connect() error
	Msg(topic string, msg []byte) error
	Close() error
}
