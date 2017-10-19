package bus

type Consumer interface {
	Connect() error
	Msg() ([]byte, error)
	Close() error
}

type Producer interface {
	Connect() error
	Send(topic string, msg []byte) error
	Close() error
}
