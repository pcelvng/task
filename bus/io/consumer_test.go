package io

import "testing"

func TestNewStdinConsumer(t *testing.T) {
	_, err := NewStdinConsumer()
	if err != nil {
		t.Fatalf("expected nil but got '%v'", err.Error())
	}

}
