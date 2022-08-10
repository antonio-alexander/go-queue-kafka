package kafka

import (
	"context"
	"encoding"
	"sync"
	"time"

	goqueue "github.com/antonio-alexander/go-queue"
	finite "github.com/antonio-alexander/go-queue/finite"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

//TODO: this should start somewhere simple, at a high level I want a
// kafka client that's wrapped in the queue interfaces

type kafkaQueue struct {
	sync.RWMutex
	sync.WaitGroup
	started       bool
	stopper       chan struct{}
	config        Configuration
	client        sarama.Client
	producer      sarama.SyncProducer
	consumerGroup sarama.ConsumerGroup
	errorHandler  ErrorHandlerFx
	queue         interface {
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
}

var _ sarama.ConsumerGroupHandler = &kafkaQueue{}

func New(parameters ...interface{}) interface {
	goqueue.Dequeuer
	goqueue.Enqueuer
	//REVIEW: do we want to implement events?
	// goqueue.Event
	goqueue.Length
	goqueue.Owner
	Owner
} {
	var config *Configuration

	k := &kafkaQueue{}
	for _, p := range parameters {
		switch v := p.(type) {
		case ErrorHandlerFx:
			k.errorHandler = v
		case *Configuration:
			config = v
		}
	}
	if config != nil {
		if err := k.Initialize(config); err != nil {
			panic(err)
		}
	}
	return k
}

func (k *kafkaQueue) error(err error) {
	if err == nil {
		return
	}
	if errorHandler := k.errorHandler; errorHandler != nil {
		errorHandler(err)
	}
}

func (k *kafkaQueue) launchConsume(consumerGroup sarama.ConsumerGroup, stopper chan struct{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	k.Add(1)
	go func() {
		defer k.Done()

		select {
		case <-stopper:
			cancel()
		case <-ctx.Done():
		}
	}()
	chErr := make(chan error)
	k.Add(1)
	go func() {
		defer k.Done()
		if err := consumerGroup.Consume(ctx, []string{k.config.TopicIn}, k); err != nil {
			cancel()
			chErr <- err
		}
	}()
	select {
	case <-time.After(time.Second):
		return nil
	case err := <-chErr:
		return err
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (k *kafkaQueue) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (k *kafkaQueue) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (k *kafkaQueue) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		session.MarkMessage(msg, "")
		MustEnqueue(k.queue, msg.Value)
	}
	return nil
}

func (k *kafkaQueue) Initialize(config *Configuration) error {
	k.Lock()
	defer k.Unlock()
	if k.started {
		return errors.New("kafka started")
	}
	if config != nil {
		k.config = *config
	}
	client, err := CreateKafkaClient(k.config)
	if err != nil {
		return err
	}
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return err
	}
	consumerGroup, err := sarama.NewConsumerGroupFromClient(k.config.GroupID, client)
	if err != nil {
		return err
	}
	if k.queue == nil {
		k.queue = finite.New(k.config.QueueSize)
	}
	stopper := make(chan struct{})
	if err := k.launchConsume(consumerGroup, stopper); err != nil {
		return err
	}
	k.client = client
	k.producer = producer
	k.consumerGroup = consumerGroup
	k.stopper = stopper
	k.started = true
	// k.launchPartitions()
	return nil
}

func (k *kafkaQueue) Shutdown() []interface{} {
	k.Lock()
	defer k.Unlock()
	if !k.started {
		return nil
	}
	close(k.stopper)
	k.Wait()
	items := k.queue.Close()
	if err := k.consumerGroup.Close(); err != nil {
		k.error(err)
	}
	if err := k.producer.Close(); err != nil {
		k.error(err)
	}
	if err := k.client.Close(); err != nil {
		k.error(err)
	}
	k.consumerGroup, k.producer, k.client = nil, nil, nil
	k.queue = nil
	k.started = false
	return items
}

func (k *kafkaQueue) Close() []interface{} {
	return k.Shutdown()
}

func (k *kafkaQueue) Dequeue() (interface{}, bool) {
	k.RLock()
	defer k.RUnlock()
	return k.queue.Dequeue()
}

func (k *kafkaQueue) DequeueMultiple(n int) []interface{} {
	return k.queue.DequeueMultiple(n)
}

func (k *kafkaQueue) Flush() (items []interface{}) {
	return k.queue.Flush()
}

func (k *kafkaQueue) Enqueue(item interface{}) bool {
	message := &sarama.ProducerMessage{
		Topic:     k.config.TopicOut,
		Key:       sarama.ByteEncoder{},
		Headers:   []sarama.RecordHeader{},
		Timestamp: time.Now(),
		Offset:    sarama.OffsetNewest,
		Partition: 0,
	}
	switch v := item.(type) {
	default:
		return true
	case encoding.BinaryMarshaler:
		bytes, err := v.MarshalBinary()
		if err != nil {
			k.error(err)
			return true
		}
		message.Value = sarama.ByteEncoder(bytes)
	case []byte:
		message.Value = sarama.ByteEncoder(v)
	}
	if _, _, err := k.producer.SendMessage(message); err != nil {
		k.error(err)
		return true
	}
	return false
}

func (k *kafkaQueue) EnqueueMultiple(items []interface{}) ([]interface{}, bool) {
	var messages []*sarama.ProducerMessage

	for _, item := range items {
		message := &sarama.ProducerMessage{
			Topic:     k.config.TopicOut,
			Key:       sarama.ByteEncoder{},
			Headers:   []sarama.RecordHeader{},
			Timestamp: time.Now(),
		}
		switch v := item.(type) {
		default:
			k.error(errors.Errorf(ErrUnsupportedTypef, v))
			return nil, true
		case encoding.BinaryMarshaler:
			bytes, err := v.MarshalBinary()
			if err != nil {
				k.error(err)
				return nil, true
			}
			message.Value = sarama.ByteEncoder(bytes)
		case []byte:
			message.Value = sarama.ByteEncoder(v)
		}
	}
	if err := k.producer.SendMessages(messages); err != nil {
		k.error(err)
		return nil, true
	}
	return nil, false
}

func (k *kafkaQueue) Length() (size int) {
	return k.queue.Length()
}
