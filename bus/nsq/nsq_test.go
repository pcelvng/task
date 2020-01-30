package nsq

import (
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/jbsmith7741/trial"
	"github.com/pkg/errors"
)

func TestGetTopics(t *testing.T) {
	httpmock.Activate()

	// nsq version 0.3.8 response
	httpmock.RegisterResponder("GET", "http://test/topics",
		httpmock.NewStringResponder(200, `{"status_code":200,"status_txt":"OK","data":{"topics":["done","task1","task2"]}}`))
	topics, err := getTopics("test")
	if err != nil {
		t.Errorf("FAIL: nsq 0.3.8 %s", err)
	} else if equal, diff := trial.Equal(topics, []string{"done", "task1", "task2"}); !equal {
		t.Errorf("FAIL: %s", diff)
	} else {
		t.Log("PASS: nsq 0.3.8")
	}

	// nsq version > 1.0.0 response
	httpmock.Reset()
	httpmock.RegisterResponder("GET", "http://test/topics",
		httpmock.NewStringResponder(200, `{"topics":["done","task1","task2"]}`))
	topics, err = getTopics("test")
	if err != nil {
		t.Errorf("FAIL: nsq 1.0.0 %s", err)
	} else if equal, diff := trial.Equal(topics, []string{"done", "task1", "task2"}); !equal {
		t.Errorf("FAIL: %s", diff)
	} else {
		t.Log("PASS: nsq 1.0.0")
	}

	// error from host
	httpmock.Reset()
	httpmock.RegisterResponder("GET", "http://test/topics",
		httpmock.NewErrorResponder(errors.New("")))
	topics, err = getTopics("test")
	if err == nil {
		t.Error("FAIL: should handle errors")
	} else {
		t.Log("PASS: http GET error")
	}

	// json error
	httpmock.Reset()
	httpmock.RegisterResponder("GET", "http://test/topics",
		httpmock.NewStringResponder(200, `asdrf`))
	topics, err = getTopics("test")
	if err == nil {
		t.Errorf("Expected json error")
	} else {
		t.Log("PASS: json error")
	}
}

func TestRegisterConsumer(t *testing.T) {
	// setup httpmock
	httpmock.Activate()
	httpmock.RegisterResponder("GET", "http://host1/topics",
		httpmock.NewStringResponder(200, `{"topics":["done","task1","task2"]}`))
	httpmock.RegisterResponder("GET", "http://host2/topics",
		httpmock.NewStringResponder(200, `{"topics":["done","task3","task4"]}`))
	httpmock.RegisterResponder("GET", "http://bad_host/topics",
		httpmock.NewErrorResponder(errors.New("bad lookupd host")))
	defer httpmock.DeactivateAndReset()

	fn := func(args ...interface{}) (interface{}, error) {
		return Topics(args[0].([]string))
	}
	cases := trial.Cases{
		"no previous topics": {
			Input:    []string{"host1"},
			Expected: []string{"done", "task1", "task2"},
		},
		"some new topics from 2 hosts": {
			Input:    []string{"host1", "host2"},
			Expected: []string{"done", "task1", "task2", "task3", "task4"},
		},
		"http error": {
			Input:     []string{"bad_host"},
			ShouldErr: true,
		},
		"same topics on 2 hosts": {
			Input:    []string{"host1", "host1"},
			Expected: []string{"done", "task1", "task2"},
		},
	}
	trial.New(fn, cases).Test(t)
}
