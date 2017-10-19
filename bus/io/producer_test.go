package io

import (
	"bufio"
	"bytes"
	"testing"
)

func TestStdoutProducer(t *testing.T) {
	p, err := NewStdoutProducer()
	if err != nil {
		t.Fatalf("expected nil but got '%v'", err.Error())
	}

	err = p.Connect()
	if err != nil {
		t.Fatalf("expected nil but got '%v'", err.Error())
	}

	// change the writer for testing
	var b bytes.Buffer
	p.writer = bufio.NewWriter(&b)

	// send one test message
	tmsg := []byte("test message")
	err = p.Send("", tmsg)
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}

	// test that message matches (with newline)
	line, err := b.ReadBytes('\n')
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}
	expected := string("test message\n")
	if expected != string(line) {
		t.Errorf("expected '%v' but got '%v'\n", expected, string(line))
	}

	// test empty message
	tmsg = []byte("")
	err = p.Send("", tmsg)
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}

	line, err = b.ReadBytes('\n')
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}
	expected = string("\n")
	if expected != string(line) {
		t.Errorf("expected '%v' but got '%v'\n", expected, string(line))
	}

	err = p.Close()
	if err != nil {
		t.Fatalf("expected nil but got '%v'\n", err.Error())
	}
}

