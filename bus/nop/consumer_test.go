package nop

import (
	"fmt"
)

func ExampleNewConsumer() {
	// showing:
	// - non-nil consumer without err

	// non-nil consumer
	// using a non-supported mock string just
	// to demonstrate that Mock property is
	// set correctly.
	c, err := NewConsumer("mock_string")
	if c == nil {
		return
	}
	fmt.Println(c.Mock) // output: mock_string
	fmt.Println(err)    // output: <nil>

	// Output:
	// mock_string
	// <nil>
}

func ExampleNewConsumerErr() {
	// showing:
	// - nil consumer with err

	// nil consumer with err
	c, err := NewConsumer("init_err")
	if err == nil {
		return
	}

	fmt.Println(c)           // output: <nil>
	fmt.Println(err.Error()) // output: init_err

	// Output:
	// <nil>
	// init_err
}

func ExampleConsumer_Msg() {
	// showing:
	// - Msg msg standard response

	c, _ := NewConsumer("")
	if c == nil {
		return
	}
	msg, done, err := c.Msg()
	fmt.Println(string(msg)) // output: {"type":"test","info":"test-info","created":"2017-01-01T00:00:01Z"}
	fmt.Println(done)        // output: false
	fmt.Println(err)         // output: <nil>

	// Output:
	// {"type":"test","info":"test-info","created":"2017-01-01T00:00:01Z"}
	// false
	// <nil>
}

func ExampleConsumer_MsgFake() {
	// showing:
	// - Msg msg custom fake response

	c, _ := NewConsumer("")
	if c == nil {
		return
	}
	origMsg := FakeMsg
	FakeMsg = []byte("fake msg")
	msg, done, err := c.Msg()
	fmt.Println(string(msg)) // output: fake msg
	fmt.Println(done)        // output: false
	fmt.Println(err)         // output: <nil>

	FakeMsg = origMsg // return to original state

	// Output:
	// fake msg
	// false
	// <nil>
}

func ExampleConsumer_MsgDone() {
	// showing:
	// - Msg returns done == true

	c, _ := NewConsumer("msg_done")
	if c == nil {
		return
	}
	msg, done, err := c.Msg()
	fmt.Println(string(msg)) // output:
	fmt.Println(done)        // output: true
	fmt.Println(err)         // output: <nil>

	// Output:
	//
	// true
	// <nil>
}

func ExampleConsumer_MsgMsgDone() {
	// showing:
	// - Msg returns non-nil msg and done == true

	c, _ := NewConsumer("msg_msg_done")
	if c == nil {
		return
	}
	msg, done, err := c.Msg()
	fmt.Println(string(msg)) // output: {"type":"test","info":"test-info","created":"2017-01-01T00:00:01Z"}
	fmt.Println(done)        // output: true
	fmt.Println(err)         // output: <nil>

	// Output:
	// {"type":"test","info":"test-info","created":"2017-01-01T00:00:01Z"}
	// true
	// <nil>
}

func ExampleConsumer_MsgErr() {
	// showing:
	// - Msg err != nil

	c, _ := NewConsumer("msg_err")
	if c == nil {
		return
	}
	msg, done, err := c.Msg()
	fmt.Println(string(msg)) // output:
	fmt.Println(done)        // output: fals
	fmt.Println(err)         // output: msg_err

	// Output:
	//
	// false
	// msg_err
}

func ExampleConsumer_Stop() {
	// showing:
	// - Stop method returns nil error

	c, _ := NewConsumer("")
	if c == nil {
		return
	}
	fmt.Println(c.Stop()) // output: <nil>

	// Output:
	// <nil>
}

func ExampleConsumer_StopErr() {
	// showing:
	// - Stop method returns nil error

	c, _ := NewConsumer("stop_err")
	if c == nil {
		return
	}
	fmt.Println(c.Stop()) // output: stop_err

	// Output:
	// stop_err
}
