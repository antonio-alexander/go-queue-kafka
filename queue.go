package kafka

import (
	"context"
	"encoding"
	"sync"
	"time"

	internal_logger "github.com/antonio-alexander/go-queue-kafka/internal/logger"
	internal_null_logger "github.com/antonio-alexander/go-queue-kafka/internal/logger/null"

	goqueue "github.com/antonio-alexander/go-queue"
	finite "github.com/antonio-alexander/go-queue/finite"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
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

type kafkaQueue struct {
	sync.RWMutex
	sync.WaitGroup
	internal_logger.Logger
	errorHandler  ErrorHandlerFx
	config        Configuration
	ctx           context.Context
	cancel        context.CancelFunc
	client        sarama.Client
	producer      sarama.SyncProducer
	consumerGroup sarama.ConsumerGroup
	initialized   bool
	queue
}

var _ sarama.ConsumerGroupHandler = &kafkaQueue{}

func New(parameters ...interface{}) interface {
	goqueue.Dequeuer
	goqueue.Enqueuer
	goqueue.Length
	goqueue.Owner
	goqueue.Peeker
	finite.Resizer
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
		case internal_logger.Logger:
			k.Logger = v
		}
	}
	if k.Logger == nil {
		k.Logger = &internal_null_logger.NullLogger{}
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
			Topic:     k.config.TopicIn,
			Key:       sarama.ByteEncoder{},
			Headers:   []sarama.RecordHeader{},
			Timestamp: time.Now(),
			Offset:    sarama.OffsetNewest,
			Partition: 0,
			Value:     byteEncoder,
		})
	}
	return k.producer.SendMessages(messages)
}

func (k *kafkaQueue) launchConsumerGroup(topic string, consumerGroup sarama.ConsumerGroup) error {
	started := make(chan struct{})
	chErr := make(chan error)
	k.Add(1)
	go func() {
		defer k.Done()
		defer func() {
			k.Printf("stopped consumer group for topic \"%s\"", topic)
		}()

		close(started)
		k.Printf("launched partition consumer group for topic \"%s\"", topic)
		//KIM: this blocks, so it needs to be in a go routine
		if err := consumerGroup.Consume(k.ctx, []string{topic}, k); err != nil {
			select {
			default:
			case chErr <- err:
			}
		}
	}()
	<-started
	select {
	default:
		close(chErr)
		return nil
	case err := <-chErr:
		return err
	}
}

func (k *kafkaQueue) error(err error) {
	if errorHandler := k.errorHandler; errorHandler != nil && err != nil {
		errorHandler(err)
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (k *kafkaQueue) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (k *kafkaQueue) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	//TODO: make this configurable
	tCommit := time.NewTicker(time.Minute)
	defer tCommit.Stop()
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
			wrapper := &Wrapper{}
			if err := wrapper.UnmarshalBinary(msg.Value); err != nil {
				session.MarkMessage(msg, "")
				k.Printf("received message that couldn't be unmarshalled: %d\n", msg.Offset)
				k.error(err)
				continue
			}
			overflow := MustEnqueue(session.Context().Done(), k.queue, wrapper)
			if overflow {
				k.Printf("overflow while trying to enqueue message: %d\n", msg.Offset)
				continue
			}
			session.MarkMessage(msg, "")
			k.Printf("received message with offset: %d\n", msg.Offset)
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
	client, err := createKafkaClient(config, k.Logger)
	if err != nil {
		return err
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
	if err := k.launchConsumerGroup(config.TopicIn, consumerGroup); err != nil {
		k.cancel()
		return err
	}
	k.consumerGroup = consumerGroup
	k.client, k.producer = client, producer
	k.config = *config
	k.initialized = true
	return nil
}

func (k *kafkaQueue) Shutdown() {
	if !k.initialized {
		return
	}
	k.cancel()
	//TODO: place data back into queue
	k.queue.Close()
	if err := k.consumerGroup.Close(); err != nil {
		k.Printf("error while closing consumer group: %s", err)
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
	k.initialized = false
}

func (k *kafkaQueue) Close() []interface{} {
	k.Shutdown()
	return nil
}

func (k *kafkaQueue) Length() (size int) {
	return k.queue.Length()
}

func (k *kafkaQueue) Enqueue(item interface{}) bool {
	var bytes []byte

	switch v := item.(type) {
	default:
		k.error(errors.Errorf(ErrUnsupportedTypef, v))
		return true
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			k.error(err)
			return true
		}
		bytes = b
	case []byte:
		bytes = v
	}
	if err := k.publish(bytes); err != nil {
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
			k.error(errors.Errorf(ErrUnsupportedTypef, v))
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
