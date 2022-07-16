package kafka

import (
	"encoding"
	"errors"
	"fmt"
	"sync"
	"time"

	goqueue "github.com/antonio-alexander/go-queue"
	finite "github.com/antonio-alexander/go-queue/finite"

	"github.com/Shopify/sarama"
)

//TODO: this should start somewhere simple, at a high level I want a
// kafka client that's wrapped in the queue interfaces

type kafkaQueue struct {
	sync.RWMutex
	sync.WaitGroup
	started  bool
	offset   int64
	stopper  chan struct{}
	config   Configuration
	client   sarama.Client
	producer sarama.SyncProducer
	consumer sarama.Consumer
	queue    interface {
		goqueue.Dequeuer
		goqueue.EnqueueInFronter
		finite.EnqueueLossy
		goqueue.Enqueuer
		goqueue.Event
		goqueue.GarbageCollecter
		goqueue.Info
		goqueue.Owner
		goqueue.Peeker
		finite.Resizer
	}
}

func New(parameters ...interface{}) interface {
	goqueue.Dequeuer
	goqueue.Enqueuer
	//REVIEW: do we want to implement events?
	// goqueue.Event
	goqueue.Info
	Owner
} {
	k := &kafkaQueue{
		config: Configuration{
			KafkaTopic:    defaultKafkaTopic,
			KafkaHost:     defaultKafkaHost,
			KafkaPort:     defaultKafkaPort,
			KafkaClientID: defaultKafkaClientID,
			QueueSize:     defaultQueueSize,
		},
	}
	for _, p := range parameters {
		switch v := p.(type) {
		case Configuration:
			k.config = v
		}
	}
	return k
}

func (k *kafkaQueue) Start(config *Configuration) error {
	k.Lock()
	defer k.Unlock()
	if k.started {
		return errors.New("kafka started")
	}
	if config != nil {
		k.config = *config
	}
	client, err := createKafkaClient(k.config)
	if err != nil {
		return err
	}
	//REVIEW: do we need a sync producer?
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return err
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return err
	}
	k.offset = 0
	k.client = client
	k.producer = producer
	k.consumer = consumer
	k.queue = finite.New(k.config.QueueSize)
	k.stopper = make(chan struct{})
	k.started = true
	return nil
}

func (k *kafkaQueue) Stop() {
	k.Lock()
	defer k.Unlock()
	if !k.started {
		return
	}
	close(k.stopper)
	k.Wait()
	if err := k.consumer.Close(); err != nil {
		fmt.Println(err)
	}
	if err := k.producer.Close(); err != nil {
		fmt.Println(err)
	}
	if err := k.client.Close(); err != nil {
		fmt.Println(err)
	}
	k.consumer, k.producer, k.client = nil, nil, nil
	k.started = false
}

func (k *kafkaQueue) Close() {
	k.Lock()
	defer k.Unlock()
	k.queue.Close()
}

func (k *kafkaQueue) Dequeue() (item interface{}, underflow bool) {
	k.RLock()
	defer k.RUnlock()

	partitions, err := k.consumer.Partitions(k.config.KafkaTopic)
	if err != nil {
		return nil, true
	}
	for _, p := range partitions {
		//REVIEW, do we have to store the offset?
		consumer, err := k.consumer.ConsumePartition(k.config.KafkaTopic, p, k.offset)
		if err != nil {
			//TODO: print error
			return nil, true
		}
		for m := range consumer.Messages() {
			//REVIEW: how do we make this only execute once...performantly
			item = m.Value
			k.offset++
			consumer.AsyncClose()
		}
	}

	return
}

func (k *kafkaQueue) DequeueMultiple(n int) (items []interface{}) {
	return nil
}

func (k *kafkaQueue) Flush() (items []interface{}) {
	return nil
}

func (k *kafkaQueue) Enqueue(item interface{}) (overflow bool) {
	k.RLock()
	defer k.RUnlock()

	message := &sarama.ProducerMessage{
		Topic:     k.config.KafkaTopic,
		Key:       nil,
		Headers:   []sarama.RecordHeader{},
		Metadata:  nil,
		Offset:    0,
		Partition: 0,
		Timestamp: time.Now(),
	}
	switch v := item.(type) {
	default:
		return true
	case encoding.BinaryMarshaler:
		bytes, err := v.MarshalBinary()
		if err != nil {
			//TODO: log error
			return true
		}
		message.Value = sarama.ByteEncoder(bytes)
	case []byte:
		message.Value = sarama.ByteEncoder(v)
	}
	if _, _, err := k.producer.SendMessage(message); err != nil {
		return true
	}
	return
}

func (k *kafkaQueue) EnqueueMultiple(items []interface{}) (itemsRemaining []interface{}, overflow bool) {
	return nil, true
}

func (k *kafkaQueue) Length() (size int) {
	return -1
}

func (k *kafkaQueue) Capacity() (capacity int) {
	return -1
}
