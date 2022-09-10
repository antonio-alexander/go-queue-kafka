package kafka

import (
	"time"

	goqueue "github.com/antonio-alexander/go-queue"
)

func MustEnqueue(done <-chan struct{}, queue interface{}, item interface{}, rate time.Duration) bool {
	switch queue := queue.(type) {
	default:
		return true
	case interface {
		goqueue.Enqueuer
		goqueue.Event
	}:
		if overflow := queue.Enqueue(item); !overflow {
			return overflow
		}
		signalOut := queue.GetSignalOut()
		for {
			select {
			case <-done:
				return queue.Enqueue(item)
			case <-signalOut:
				if overflow := queue.Enqueue(item); !overflow {
					return overflow
				}
			}
		}
	case goqueue.Enqueuer:
		if overflow := queue.Enqueue(item); !overflow {
			return overflow
		}
		tEnqueue := time.NewTicker(rate)
		defer tEnqueue.Stop()
		for {
			select {
			case <-done:
				return queue.Enqueue(item)
			case <-tEnqueue.C:
				if overflow := queue.Enqueue(item); !overflow {
					return overflow
				}
			}
		}
	}
}

func MustDequeue(done <-chan struct{}, queue interface{}, rate time.Duration) (item interface{}, underflow bool) {
	switch queue := queue.(type) {
	default:
		return nil, true
	case interface {
		goqueue.Dequeuer
		goqueue.Event
	}:
		if item, underflow = queue.Dequeue(); !underflow {
			return
		}
		signalIn := queue.GetSignalIn()
		for {
			select {
			case <-done:
				return queue.Dequeue()
			case <-signalIn:
				if item, underflow = queue.Dequeue(); !underflow {
					return
				}
			}
		}
	case goqueue.Dequeuer:
		if item, underflow = queue.Dequeue(); !underflow {
			return
		}
		tDequeue := time.NewTicker(rate)
		defer tDequeue.Stop()
		for {
			select {
			case <-done:
				return queue.Dequeue()
			case <-tDequeue.C:
				if item, underflow = queue.Dequeue(); !underflow {
					return
				}
			}
		}
	}
}
