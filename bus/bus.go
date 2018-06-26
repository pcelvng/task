package bus

import "github.com/pcelvng/task/bus/info"

type Consumer interface {
	// Msg returns the bus msg bytes. If msg is
	// known to be the last then done should be true. msg may be
	// nil and done can be true. err should never be
	// io.EOF.
	//
	// Once the last message has been received, subsequent
	// calls to Msg should not block and always return
	// msg == nil (or len == 0), done == true and err == nil.
	//
	// A call to Msg should block until either a msg
	// is received or Stop has been called.
	//
	// Once Stop has been called subsequent calls to Msg
	// should not block and immediately return with
	// msg == nil (or len == 0), done == true and err == nil.
	Msg() (msg []byte, done bool, err error)
	Stop() error
	Info() info.Consumer
}

type Producer interface {
	Send(topic string, msg []byte) error
	Stop() error
	Info() info.Producer
}
