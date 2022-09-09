package kafka

import (
	goqueue "github.com/antonio-alexander/go-queue"
)

type Example struct {
	goqueue.Example
}

var _ Wrappable = &Example{}

func (e *Example) Type() string {
	return "Example"
}
