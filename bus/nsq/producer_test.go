package nsqbus

import (
	"testing"
)

func TestNewProducer(t *testing.T) {
	conf := &Config{}
	p := NewProducer(conf)

	// test not nil
	if p == nil {
		t.Fatal("nsq producer should not be nil")
	}

	// test has conf
	if p.conf == nil {
		t.Error("nsq producer should have conf")
	}

	// test has nsqConf
	if p.nsqConf == nil {
		t.Error("nsq producer should have nsqConf")
	}

	// test has 3 conns
	expected := 3
	if p.numConns != expected {
		t.Errorf("expected '%v' numConss but got '%v'", expected, p.numConns)
	}

	// test Connect with no hosts
	err := p.Connect()
	if err == nil {
		t.Error("expected err but got nil")
	}
}

func TestProducer(t *testing.T) {
	// connect with bad address
	//conf := &Config{
	//	NSQdAddrs: []string{"localhost:4000"},
	//}
	//p := NewProducer(conf)
	//err := p.Connect()
	//if err == nil {
	//	t.Errorf("expected nil but got err '%v'\n", err.Error())
	//}

	// connect with good address
	conf := &Config{
		NSQdAddrs: []string{"127.0.0.1:4150"},
	}
	p := NewProducer(conf)

	err := p.Connect()
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	err = p.Send("testtopic2", []byte("test message"))
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	// connect to multiple nsqds

}
