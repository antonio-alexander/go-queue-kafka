package kafka

import (
	"context"
	"encoding"
	"sync"
	"time"

	internal "github.com/antonio-alexander/go-queue-kafka/internal"

	goqueue "github.com/antonio-alexander/go-queue"
	finite "github.com/antonio-alexander/go-queue/finite"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type kafkaQueue struct {
	sync.RWMutex
	sync.WaitGroup
	internal.Logger
	errorHandler     ErrorHandlerFx
	config           Configuration
	ctx              context.Context
	cancel           context.CancelFunc
	client           sarama.Client
	producer         sarama.SyncProducer
	consumerGroup    sarama.ConsumerGroup
	initialized      bool
	chReadyToConsume chan struct{}
	queue
}

func New(parameters ...interface{}) interface {
	goqueue.Dequeuer
	goqueue.Enqueuer
	goqueue.Length
	goqueue.Owner
	goqueue.Peeker
	goqueue.Event
	goqueue.GarbageCollecter
	Owner
} {
	var config *Configuration

	k := &kafkaQueue{queue: finite.New(1)}
	for _, p := range parameters {
		switch v := p.(type) {
		case ErrorHandlerFx:
			k.errorHandler = v
		case *Configuration:
			config = v
		case internal.Logger:
			k.Logger = v
		}
	}
	if k.Logger == nil {
		k.Logger = &internal.NullLogger{}
	}
	if config != nil {
		if err := k.Initialize(config); err != nil {
			panic(err)
		}
	}
	return k
}

func (k *kafkaQueue) publish(items ...interface{}) error {
	var messages []*sarama.ProducerMessage

	if !k.initialized {
		return errors.New("not initialized")
	}
	for _, item := range items {
		var byteEncoder sarama.Encoder

		switch v := item.(type) {
		default:
			return errors.New("unsupported type")
		case encoding.BinaryMarshaler:
			bytes, err := v.MarshalBinary()
			if err != nil {
				return err
			}
			byteEncoder = sarama.ByteEncoder(bytes)
		case []byte:
			byteEncoder = sarama.ByteEncoder(v)
		}
		messages = append(messages, &sarama.ProducerMessage{
			Topic: k.config.TopicIn,
			Value: byteEncoder,
		})
	}
	return k.producer.SendMessages(messages)
}

func (k *kafkaQueue) error(err error) {
	if errorHandler := k.errorHandler; errorHandler != nil && err != nil {
		errorHandler(err)
	}
}

func (k *kafkaQueue) setReady() {
	k.Lock()
	defer k.Unlock()
	select {
	default:
		close(k.chReadyToConsume)
	case <-k.chReadyToConsume:
	}
}

func (k *kafkaQueue) unsetReady() {
	k.Lock()
	defer k.Unlock()
	select {
	default:
	case <-k.chReadyToConsume:
		k.chReadyToConsume = make(chan struct{})
	}
}

func (k *kafkaQueue) launchConsumerGroup() {
	started := make(chan struct{})
	k.Add(1)
	go func() {
		defer k.Done()

		k.chReadyToConsume = make(chan struct{})
		close(started)
		for {
			if err := k.consumerGroup.Consume(k.ctx, []string{k.config.TopicIn}, k); err != nil {
				k.errorHandler(err)
				return
			}
			if k.ctx.Err() != nil {
				return
			}
		}
	}()
	<-started
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (k *kafkaQueue) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (k *kafkaQueue) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	commitRate := k.config.CommitRate
	if commitRate <= 0 {
		commitRate = DefaultCommitRate
	}
	tCommit := time.NewTicker(commitRate)
	defer tCommit.Stop()
	k.setReady()
	defer k.unsetReady()
	for {
		select {
		case <-session.Context().Done():
			return nil
		case <-tCommit.C:
			k.Printf("committing offset\n")
			session.Commit()
		case msg := <-claim.Messages():
			if msg == nil {
				continue
			}
			overflow := goqueue.MustEnqueueEvent(k.queue, msg.Value, session.Context().Done())
			if overflow {
				//KIM: this is semi-catastrophic and probably needs to be handled better
				// this would only be an issue if the queue was being closed while
				// the queue was full; it shouldn't mark the message
				k.Printf("overflow while trying to enqueue message: %d\n", msg.Offset)
				continue
			}
			session.MarkMessage(msg, "")
			k.Printf(k.config.ClientId+": received message with offset: %d\n", msg.Offset)
		}
	}
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (k *kafkaQueue) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (k *kafkaQueue) Initialize(config *Configuration) error {
	if k.initialized {
		return errors.New("kafka already initialized")
	}
	if config == nil {
		return errors.New("config is nil")
	}
	client, err := sarama.NewClient(config.ToKafka())
	if err != nil {
		return err
	}
	if config.EnableLog {
		sarama.Logger = k.Logger
	}
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return err
	}
	consumerGroup, err := sarama.NewConsumerGroupFromClient(config.GroupId, client)
	if err != nil {
		return err
	}
	k.queue.Resize(config.QueueSize)
	k.ctx, k.cancel = context.WithCancel(context.Background())
	k.consumerGroup = consumerGroup
	k.client, k.producer = client, producer
	k.config = *config
	k.launchConsumerGroup()
	<-k.chReadyToConsume
	k.initialized = true
	return nil
}

func (k *kafkaQueue) Shutdown() {
	if !k.initialized {
		return
	}
	k.cancel()
	k.Wait()
	if err := k.consumerGroup.Close(); err != nil {
		k.Printf("error while closing consumer group: %s", err)
	}
	if items := k.queue.Close(); len(items) > 0 {
		k.Printf("items in queue, attempting to re-publish")
		if err := k.publish(items...); err != nil {
			k.Printf("error while re-publishing in-memory items: %s", err)
		}
	}
	if err := k.producer.Close(); err != nil {
		k.Printf("error while closing producer: %s", err)
	}
	if err := k.client.Close(); err != nil {
		k.Printf("error while closing client: %s", err)
	}
	k.producer, k.client = nil, nil
	k.consumerGroup = nil
	k.initialized = false
}

func (k *kafkaQueue) Close() []interface{} {
	k.Shutdown()
	return nil
}

func (k *kafkaQueue) Enqueue(item interface{}) bool {
	var message interface{}

	switch v := item.(type) {
	default:
		k.error(errors.Errorf(UnsupportedTypef, v))
		return true
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			k.error(err)
			return true
		}
		message = b
	case []byte:
		message = v
	}
	if err := k.publish(message); err != nil {
		k.error(err)
		return true
	}
	return false
}

func (k *kafkaQueue) EnqueueMultiple(items []interface{}) ([]interface{}, bool) {
	var messages []interface{}

	for _, item := range items {
		switch v := item.(type) {
		default:
			k.error(errors.Errorf(UnsupportedTypef, v))
			return nil, true
		case encoding.BinaryMarshaler:
			bytes, err := v.MarshalBinary()
			if err != nil {
				k.error(err)
				return nil, true
			}
			messages = append(messages, bytes)
		case []byte:
			messages = append(messages, v)
		}
	}
	if err := k.publish(messages...); err != nil {
		k.error(err)
		return nil, true
	}
	return nil, false
}
