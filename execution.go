package kafka

import (
	"time"

	internal_logger "github.com/antonio-alexander/go-queue-kafka/internal/logger"

	goqueue "github.com/antonio-alexander/go-queue"

	"github.com/Shopify/sarama"
)

func createKafkaClient(c *Configuration, logger internal_logger.Logger) (sarama.Client, error) {
	config := sarama.NewConfig()
	config.ClientID = c.ClientId
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.ChannelBufferSize = 1024
	config.Consumer.Return.Errors = true
	client, err := sarama.NewClient(c.Brokers, config)
	if err != nil {
		return nil, err
	}
	if c.EnableLog {
		sarama.Logger = logger
	}
	return client, nil
}

func MustEnqueue(done <-chan struct{}, queue interface{}, item interface{}) bool {
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
		//TODO: make this configurable
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
		//TODO: make this configurable
		tEnqueue := time.NewTicker(time.Second)
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
	return true
}

func MustDequeue(done <-chan struct{}, queue interface{}) (item interface{}, underflow bool) {
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
		tDequeue := time.NewTicker(time.Millisecond)
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
