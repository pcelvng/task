package io

import "testing"

func TestNewStdinConsumer(t *testing.T) {
	c := NewStdinConsumer()
	if c == nil {
		t.Error("expected consumer but got nil instead")
	}
}
