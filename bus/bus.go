package bus

type ConsumerBus interface {
	Msg() (msg []byte, done bool, err error)
	Stop() error
}

type ProducerBus interface {
	Send(topic string, msg []byte) error
	Stop() error
}
