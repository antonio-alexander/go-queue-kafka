package kafka_test

import (
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	kafka "github.com/antonio-alexander/go-queue-kafka"
	internal "github.com/antonio-alexander/go-queue-kafka/internal"
	pb "github.com/antonio-alexander/go-queue-kafka/protos"

	goqueue "github.com/antonio-alexander/go-queue"
	infinite_tests "github.com/antonio-alexander/go-queue/infinite/tests"
	goqueue_tests "github.com/antonio-alexander/go-queue/tests"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	mustRate    = time.Second
	mustTimeout = 5 * time.Second
)

var kafkaQueueConfig = &kafka.Configuration{}

type kafkaQueueTest struct {
	logger internal.Logger
	config *kafka.Configuration
}

func init() {
	//seed the random package
	rand.Seed(time.Now().UnixNano())

	//get the environment
	envs := make(map[string]string)
	for _, s := range os.Environ() {
		if s := strings.Split(s, "="); len(s) > 1 {
			envs[s[0]] = strings.Join(s[1:], "=")
		}
	}

	//update with some known constants
	kafkaQueueConfig.TopicIn = "queue_in"
	kafkaQueueConfig.TopicOut = "queue_in"
	kafkaQueueConfig.Brokers = []string{"localhost:9092"}
	kafkaQueueConfig.QueueSize = 1000
	kafkaQueueConfig.EnableLog = true

	//generate the configuration from the environment
	kafkaQueueConfig.FromEnv(envs)
}

//REFERENCE: https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
func randomString(nLetters ...int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	nLetter := 20
	if len(nLetters) > 0 {
		nLetter = nLetters[0]
	}
	b := make([]rune, nLetter)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func newKafkaServiceTest(t *testing.T) *kafkaQueueTest {
	return &kafkaQueueTest{
		logger: log.New(os.Stdout, "", log.Ltime),
		config: kafkaQueueConfig,
	}
}

func (k *kafkaQueueTest) errorHandlerFx(t *testing.T) func(err error) {
	return func(err error) {
		assert.Nil(t, err)
	}
}

func (k *kafkaQueueTest) TestConsumerGroup(t *testing.T) {
	const nExamples = 10

	var wg sync.WaitGroup
	var mu sync.RWMutex

	//use sarama admin to create a new topic
	topic := uuid.Must(uuid.NewRandom()).String()
	k.config.TopicIn, k.config.TopicOut = topic, topic
	k.config.ClientId = uuid.Must(uuid.NewRandom()).String()
	admin, err := sarama.NewClusterAdmin(k.config.ToKafka())
	defer func() { admin.Close() }()
	assert.Nil(t, err)
	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}, false)
	assert.Nil(t, err)
	err = admin.Close()
	assert.Nil(t, err)

	//create two queues with the same group id, but different client ids
	k.config.GroupId = uuid.Must(uuid.NewRandom()).String()

	//initialize the first queue/consumer group
	q1 := kafka.New(k.errorHandlerFx(t), k.logger)
	k.config.ClientId = uuid.Must(uuid.NewRandom()).String()
	err = q1.Initialize(k.config)
	assert.Nil(t, err)
	defer func() { q1.Shutdown() }()

	//initialize the second queue/consumer group
	q2 := kafka.New(k.errorHandlerFx(t), k.logger)
	k.config.ClientId = uuid.Must(uuid.NewRandom()).String()
	err = q2.Initialize(k.config)
	assert.Nil(t, err)
	defer func() { q2.Shutdown() }()

	//
	examplesRead := make(map[kafka.Example]struct{})
	examplesWritten := make(map[kafka.Example]struct{})
	start := make(chan struct{})
	stopper := make(chan struct{})
	messagesRead := make(chan struct{})
	messagesWritten := make(chan struct{})

	//use one of the queues to enqueue data
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-start
		//KIM: without adding some call/response code, there's no synchronous way
		// to ensure that the newly created consumer group is receiving these
		// published items
		time.Sleep(5 * time.Second)
		for i := 0; i < nExamples; i++ {
			exampleWritten := &kafka.Example{
				&pb.Example{
					Int:     rand.Int31(),
					Float64: rand.Float64(),
					String_: randomString(),
				},
			}
			goqueue.MustEnqueueEvent(q1, exampleWritten, stopper)
			examplesWritten[*exampleWritten] = struct{}{}
		}
		close(messagesWritten)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-start
		for {
			select {
			case <-stopper:
				return
			case <-messagesRead:
				return
			default:
				item, underflow := goqueue.MustDequeueEvent(q1, stopper)
				if underflow {
					continue
				}
				bytes, _ := item.([]byte)
				exampleRead := &kafka.Example{}
				if err := exampleRead.UnmarshalBinary(bytes); err == nil {
					mu.Lock()
					examplesRead[*exampleRead] = struct{}{}
					select {
					default:
					case <-messagesWritten:
						if len(examplesRead) == len(examplesWritten) {
							select {
							default:
								close(messagesRead)
							case <-messagesRead:
							}
						}
					}
					mu.Unlock()
				}
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-start
		for {
			select {
			case <-stopper:
				return
			default:
				item, underflow := goqueue.MustDequeueEvent(q2, stopper)
				if underflow {
					continue
				}
				bytes, _ := item.([]byte)
				exampleRead := &kafka.Example{}
				if err := exampleRead.UnmarshalBinary(bytes); err == nil {
					mu.Lock()
					examplesRead[*exampleRead] = struct{}{}
					select {
					default:
					case <-messagesWritten:
						if len(examplesRead) == len(examplesWritten) {
							select {
							default:
								close(messagesRead)
							case <-messagesRead:
							}
						}
					}
					mu.Unlock()
				}
			}
		}
	}()
	close(start)
	select {
	case <-time.After(10 * time.Second):
	case <-messagesRead:
	}
	close(stopper)
	wg.Wait()

	//validate that all data was received (the order is irrelevant)
	assert.Equal(t, len(examplesRead), len(examplesWritten))
	assert.Condition(t, func() bool {
		if len(examplesWritten) != len(examplesRead) {
			return false
		}
		for exampleRead := range examplesRead {
			if _, ok := examplesWritten[exampleRead]; !ok {
				return false
			}
		}
		return true
	})

	//shutdown
	q1.Shutdown()
	q2.Shutdown()
}

func (k *kafkaQueueTest) TestQueue(t *testing.T) {
	var topics []string

	//KIM: the premise of most of these tests is that although the
	// length is infinite, the order should be maintained, so we
	// absolutely can't load balance across multiple partitions
	// or the tests will fail due to behavior issues (all of the
	// topics we'll create have a single partition)

	//create the kafka admin so we can create topics
	k.config.ClientId = uuid.Must(uuid.NewRandom()).String()
	admin, err := sarama.NewClusterAdmin(k.config.ToKafka())
	defer func() {
		for _, topic := range topics {
			err := admin.DeleteTopic(topic)
			assert.Nil(t, err)
		}
		err = admin.Close()
		assert.Nil(t, err)
	}()
	assert.Nil(t, err)

	newKafkaQueue := func(size ...int) interface {
		goqueue.Dequeuer
		goqueue.Enqueuer
		goqueue.Event
		goqueue.Length
		goqueue.Owner
		goqueue.Peeker
	} {
		//use sarama admin to create a new topic
		topic := uuid.Must(uuid.NewRandom()).String()
		topics = append(topics, topic)
		err = admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		assert.Nil(t, err)
		//create a new/unique group/client id every time so we don't have
		// to flush
		k.config.ClientId = uuid.Must(uuid.NewRandom()).String()
		k.config.GroupId = uuid.Must(uuid.NewRandom()).String()
		k.config.TopicIn, k.config.TopicOut = topic, topic
		if len(size) > 0 {
			k.config.QueueSize = size[0]
		}
		k.config.EnableLog = false
		parameters := []interface{}{
			k.errorHandlerFx(t),
			k.config,
			// k.logger,
		}
		return kafka.New(parameters...)
	}
	t.Run("Peek", goqueue_tests.TestPeek(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Peeker
	} {
		return newKafkaQueue(size)
	}))
	t.Run("Peek From Head", goqueue_tests.TestPeekFromHead(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Peeker
	} {
		return newKafkaQueue(size)
	}))
	t.Run("Event", goqueue_tests.TestEvent(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Event
	} {
		return newKafkaQueue(size)
	}))
	// t.Run("Queue", goqueue_tests.TestQueue(t, mustRate, mustTimeout, func(size int) interface {
	// 	goqueue.Owner
	// 	goqueue.Enqueuer
	// 	goqueue.Dequeuer
	// } {
	// 	return newKafkaQueue(size)
	// }))
	t.Run("Asynchronous", goqueue_tests.TestAsync(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
	} {
		return newKafkaQueue(size)
	}))
}

func (k *kafkaQueueTest) TestFiniteQueue(t *testing.T) {
	var topics []string

	//KIM: the premise of most of these tests is that although the
	// length is infinite, the order should be maintained, so we
	// absolutely can't load balance across multiple partitions
	// or the tests will fail due to behavior issues (all of the
	// topics we'll create have a single partition)

	//create the kafka admin so we can create topics
	k.config.ClientId = uuid.Must(uuid.NewRandom()).String()
	admin, err := sarama.NewClusterAdmin(k.config.ToKafka())
	defer func() {
		for _, topic := range topics {
			err := admin.DeleteTopic(topic)
			assert.Nil(t, err)
		}
		err = admin.Close()
		assert.Nil(t, err)
	}()
	assert.Nil(t, err)

	newKafkaQueue := func(size ...int) interface {
		goqueue.Dequeuer
		goqueue.Enqueuer
		goqueue.Event
		goqueue.Length
		goqueue.Owner
		goqueue.Peeker
	} {
		//use sarama admin to create a new topic
		topic := uuid.Must(uuid.NewRandom()).String()
		topics = append(topics, topic)
		err = admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		assert.Nil(t, err)
		//create a new/unique group/client id every time so we don't have
		// to flush
		k.config.ClientId = uuid.Must(uuid.NewRandom()).String()
		k.config.GroupId = uuid.Must(uuid.NewRandom()).String()
		k.config.TopicIn, k.config.TopicOut = topic, topic
		if len(size) > 0 {
			k.config.QueueSize = size[0]
		}
		k.config.EnableLog = false
		parameters := []interface{}{
			k.errorHandlerFx(t),
			k.config,
			// k.logger,
		}
		return kafka.New(parameters...)
	}
	t.Run("Dequeue", goqueue_tests.TestDequeue(t, mustRate, mustTimeout, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
	} {
		return newKafkaQueue(size)
	}))
	t.Run("Dequeue Multiple", goqueue_tests.TestDequeueMultiple(t, mustRate, mustTimeout, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
	} {
		return newKafkaQueue(size)
	}))
	t.Run("Flush", goqueue_tests.TestFlush(t, mustRate, mustTimeout, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
	} {
		return newKafkaQueue(size)
	}))
}

func (k *kafkaQueueTest) TestInfiniteQueue(t *testing.T) {
	var topics []string

	//KIM: the premise of most of these tests is that although the
	// length is infinite, the order should be maintained, so we
	// absolutely can't load balance across multiple partitions
	// or the tests will fail due to behavior issues (all of the
	// topics we'll create have a single partition)

	//create the kafka admin so we can create topics
	k.config.ClientId = uuid.Must(uuid.NewRandom()).String()
	admin, err := sarama.NewClusterAdmin(k.config.ToKafka())
	defer func() {
		for _, topic := range topics {
			err := admin.DeleteTopic(topic)
			assert.Nil(t, err)
		}
		err = admin.Close()
		assert.Nil(t, err)
	}()
	assert.Nil(t, err)

	newKafkaQueue := func(size ...int) interface {
		goqueue.Dequeuer
		goqueue.Enqueuer
		goqueue.Event
		goqueue.Length
		goqueue.Owner
		goqueue.Peeker
	} {
		//use sarama admin to create a new topic
		topic := uuid.Must(uuid.NewRandom()).String()
		topics = append(topics, topic)
		err = admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		assert.Nil(t, err)
		//create a new/unique group/client id every time so we don't have
		// to flush
		k.config.ClientId = uuid.Must(uuid.NewRandom()).String()
		k.config.GroupId = uuid.Must(uuid.NewRandom()).String()
		k.config.TopicIn, k.config.TopicOut = topic, topic
		if len(size) > 0 {
			k.config.QueueSize = size[0]
		}
		k.config.EnableLog = false
		parameters := []interface{}{
			k.errorHandlerFx(t),
			k.config,
			// k.logger,
		}
		return kafka.New(parameters...)
	}

	t.Run("Enqueue", infinite_tests.TestEnqueue(t, mustRate, mustTimeout, func() interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
	} {
		return newKafkaQueue()
	}))
	t.Run("Enqueue Multiple", infinite_tests.TestEnqueueMultiple(t, mustRate, mustTimeout, func() interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
	} {
		return newKafkaQueue()
	}))
}

func TestKafkaQueue(t *testing.T) {
	k := newKafkaServiceTest(t)

	t.Run("Test Consumer Group", k.TestConsumerGroup)
	// t.Run("Test Infinite Queue", k.TestInfiniteQueue)
	// t.Run("Test Finite Queue", k.TestFiniteQueue)
	// t.Run("Test Queue", k.TestQueue)
}
