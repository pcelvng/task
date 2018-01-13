package nop

import "fmt"

func ExampleNewProducer() {
	// showing:
	// - non-nil producer without err

	// non-nil producer
	// using a non-supported mock string just
	// to demonstrate that Mock property is
	// set correctly.
	p, err := NewProducer("mock_string")
	if p == nil {
		return
	}
	fmt.Println(p.Mock) // output: mock_string
	fmt.Println(err)    // output: <nil>

	// Output:
	// mock_string
	// <nil>
}

func ExampleNewProducerErr() {
	// showing:
	// - nil producer with err

	// nil producer with err
	p, err := NewProducer("init_err")
	if err == nil {
		return
	}

	fmt.Println(p)           // output: <nil>
	fmt.Println(err.Error()) // output: init_err

	// Output:
	// <nil>
	// init_err
}

func ExampleProducer_Send() {
	// showing:
	// - Send method returns nil error

	p, _ := NewProducer("")
	if p == nil {
		return
	}
	fmt.Println(p.Send("test-topic", []byte("test-msg"))) // output: <nil>

	// Output:
	// <nil>
}

func ExampleProducer_SendErr() {
	// showing:
	// - Send method returns non-nil error

	p, _ := NewProducer("send_err")
	if p == nil {
		return
	}
	fmt.Println(p.Send("test-topic", []byte("test-msg"))) // output: send_err

	// Output:
	// send_err
}

func ExampleProducer_Stop() {
	// showing:
	// - Stop method returns nil error

	p, _ := NewProducer("")
	if p == nil {
		return
	}
	fmt.Println(p.Stop()) // output: <nil>

	// Output:
	// <nil>
}

func ExampleProducer_StopErr() {
	// showing:
	// - Stop method returns nil error

	p, _ := NewProducer("stop_err")
	if p == nil {
		return
	}
	fmt.Println(p.Stop()) // output: stop_err

	// Output:
	// stop_err
}
