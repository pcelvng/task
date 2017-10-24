package bus

type Consumer interface {
	Connect(topic, channel string) error
	Msg() (msg []byte, done bool, err error)
	Close() error
}

type Producer interface {
	Connect() error
	Send(topic string, msg []byte) error
	Close() error
}
