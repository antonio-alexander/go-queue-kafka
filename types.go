package kafka

import (
	goqueue "github.com/antonio-alexander/go-queue"
	finite "github.com/antonio-alexander/go-queue/finite"
)

type queue interface {
	finite.Capacity
	goqueue.Dequeuer
	goqueue.EnqueueInFronter
	finite.EnqueueLossy
	goqueue.Enqueuer
	goqueue.Event
	goqueue.GarbageCollecter
	goqueue.Length
	goqueue.Owner
	goqueue.Peeker
	finite.Resizer
}

type ErrorHandlerFx func(error)

type Owner interface {
	Initialize(config *Configuration) (err error)
	Shutdown()
}
