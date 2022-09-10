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

	goqueue "github.com/antonio-alexander/go-queue"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var kafkaQueueConfig = &kafka.Configuration{}

type kafkaQueueTest struct {
	logger internal.Logger
	queue  interface {
		goqueue.Dequeuer
		goqueue.Enqueuer
		goqueue.Length
		goqueue.Owner
		kafka.Owner
	}
}

func init() {
	envs := make(map[string]string)
	for _, s := range os.Environ() {
		if s := strings.Split(s, "="); len(s) > 1 {
			envs[s[0]] = strings.Join(s[1:], "=")
		}
	}

	kafkaQueueConfig.FromEnv(envs)

	//
	kafkaQueueConfig.TopicIn = "queue_in"
	kafkaQueueConfig.TopicOut = "queue_in"
	kafkaQueueConfig.Brokers = []string{"localhost:9092"}
	kafkaQueueConfig.QueueSize = 1000
	kafkaQueueConfig.ClientId = uuid.Must(uuid.NewRandom()).String()
	kafkaQueueConfig.GroupId = uuid.Must(uuid.NewRandom()).String()
	kafkaQueueConfig.ConsumerGroup = false
	kafkaQueueConfig.EnableLog = true

	//
	rand.Seed(time.Now().UnixNano())
}

func errorHandlerFx(t *testing.T) func(err error) {
	return func(err error) {
		t.Log(err)
	}
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
	logger := log.New(os.Stdout, "", log.Ltime)
	queue := kafka.New(logger, errorHandlerFx(t))
	//KIM: if we don't flush because sql is persistent
	// all the tests will fail because of data from a
	// previous test
	return &kafkaQueueTest{
		queue:  queue,
		logger: logger,
	}
}

func (k *kafkaQueueTest) initialize(t *testing.T) {
	err := k.queue.Initialize(kafkaQueueConfig)
	assert.Nil(t, err)
}

func (k *kafkaQueueTest) shutdown(t *testing.T) {
	k.queue.Shutdown()
}

func (k *kafkaQueueTest) TestSimple(t *testing.T) {
	var wg sync.WaitGroup

	example := &kafka.Example{
		Example: goqueue.Example{
			Int:    rand.Int(),
			Float:  rand.Float64(),
			String: randomString(25),
		},
	}
	start := make(chan struct{})
	stopper := make(chan struct{})
	wrapperReceived := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()

		wrapper := &kafka.Wrapper{}
		wrapper.ToWrapper(example)
		tEnqueue := time.NewTicker(time.Second)
		defer tEnqueue.Stop()
		<-start
		for {
			select {
			case <-stopper:
				return
			case <-wrapperReceived:
				return
			case <-tEnqueue.C:
				overflow := k.queue.Enqueue(wrapper)
				if overflow {
					t.Log("overflow here...")
				}
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()

		tDequeue := time.NewTicker(time.Second)
		defer tDequeue.Stop()
		<-start
		for {
			select {
			case <-stopper:
				return
			case <-tDequeue.C:
				item, _ := k.queue.Dequeue()
				wrapperRead, ok := item.(*kafka.Wrapper)
				if !ok {
					continue
				}
				exampleRead := &kafka.Example{}
				if exampleRead.Type() != wrapperRead.Type {
					continue
				}
				err := exampleRead.UnmarshalBinary(wrapperRead.Bytes)
				if err != nil {
					continue
				}
				if !assert.Equal(t, exampleRead, example) {
					continue
				}
				close(wrapperReceived)
			}
		}
	}()
	close(start)
	select {
	case <-time.After(10 * time.Second):
		assert.Fail(t, "unable to confirm wrapper received")
	case <-wrapperReceived:
	}
	close(stopper)
	wg.Wait()
}

func (k *kafkaQueueTest) TestEnqueue(t *testing.T) {
	//
	time.Sleep(2 * time.Second)
	//
	for i := 0; i < 5; i++ {
		wrapper := &kafka.Wrapper{}
		err := wrapper.ToWrapper(&kafka.Example{
			Example: goqueue.Example{
				Int: i,
			},
		})
		assert.Nil(t, err)
		bytes, err := wrapper.MarshalBinary()
		assert.Nil(t, err)
		overflow := kafka.MustEnqueue(make(<-chan struct{}), k.queue, bytes, time.Second)
		assert.False(t, overflow)
	}

	//
	for i := 0; i < 5; i++ {
		item, underflow := kafka.MustDequeue(make(chan struct{}), k.queue, time.Second)
		assert.False(t, underflow)
		bytes, ok := item.([]byte)
		assert.True(t, ok)
		wrapper := &kafka.Wrapper{}
		err := wrapper.UnmarshalBinary(bytes)
		assert.Nil(t, err)
		item, err = wrapper.FromWrapper()
		assert.Nil(t, err)
		assert.IsType(t, &kafka.Example{}, item)
		example, _ := item.(*kafka.Example)
		assert.Equal(t, i, example.Int)
	}
}

//TODO: these are the things I want to test
// How multiple queues work with consumer groups
// General latency between enquing and dequeueing
// How to properly peek, what are the mitigation strategies for data loss
// how we can peek using the kafka client without marking the message as read
// how we can know how many messages are available to read

func TestKafkaQueue(t *testing.T) {
	k := newKafkaServiceTest(t)

	k.initialize(t)
	// t.Run("Test Simple", k.TestSimple)
	t.Run("Test Enqueue", k.TestEnqueue)
	k.shutdown(t)
}
