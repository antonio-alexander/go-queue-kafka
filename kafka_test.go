package kafka_test

import (
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	goqueue "github.com/antonio-alexander/go-queue"
	kafka "github.com/antonio-alexander/go-queue-kafka"
	pb "github.com/antonio-alexander/go-queue-kafka/protos"

	infinite_tests "github.com/antonio-alexander/go-queue/infinite/tests"
	goqueue_tests "github.com/antonio-alexander/go-queue/tests"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var configuration = &kafka.Configuration{}

func init() {
	envs := make(map[string]string)
	for _, s := range os.Environ() {
		if s := strings.Split(s, "="); len(s) > 1 {
			envs[s[0]] = strings.Join(s[1:], "=")
		}
	}
	configuration = kafka.ConfigFromEnv(envs)
	configuration.TopicIn = "queue_in"
	configuration.TopicOut = "queue_in"
	configuration.Port = "9093"
	configuration.QueueSize = 1000
	configuration.ClientID = uuid.Must(uuid.NewRandom()).String()
	configuration.GroupID = uuid.Must(uuid.NewRandom()).String()
}

func new(t *testing.T, configuration *kafka.Configuration) interface {
	goqueue.Dequeuer
	goqueue.Enqueuer
	goqueue.Owner
	// goqueue.Peeker
	goqueue.Length
	kafka.Owner
} {
	errorHandlerFx := kafka.ErrorHandlerFx(func(err error) { t.Log(err) })
	kafka := kafka.New(configuration, errorHandlerFx)
	//KIM: if we don't flush because sql is persistent
	// all the tests will fail because of data from a
	// previous test
	kafka.Flush()
	return kafka
}

func TestKafkaDequeue(t *testing.T) {
	var wg sync.WaitGroup

	sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	k := kafka.New(configuration,
		kafka.ErrorHandlerFx(func(err error) {
			t.Logf("Error: %s", err)
		}))
	assert.NotNil(t, k)
	wrapper := &pb.Wrapper{
		Type: "type",
	}
	bytes, err := proto.Marshal(wrapper)
	assert.Nil(t, err)
	assert.NotEmpty(t, bytes)
	start := make(chan struct{})
	received := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()

		tEnqueue := time.NewTicker(time.Second)
		defer tEnqueue.Stop()
		<-start
		for {
			select {
			case <-tEnqueue.C:
				kafka.MustEnqueue(k, bytes)
			case <-received:
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-start
		item := kafka.MustDequeue(k)
		assert.NotNil(t, item)
		bytes, ok := item.([]byte)
		if assert.True(t, ok) {
			wrapperRead := &pb.Wrapper{}
			err = proto.Unmarshal(bytes, wrapperRead)
			assert.Nil(t, err)
			assert.Equal(t, wrapper.Type, wrapperRead.Type)
		}
		close(received)
	}()
	close(start)
	wg.Wait()
	k.Close()
}

func TestKafkaQueue(t *testing.T) {
	t.Run("Test New", func(t *testing.T) {
		infinite_tests.TestNew(t, func(size int) interface {
			goqueue.Owner
			goqueue.Length
		} {
			return new(t, configuration)
		})
	})
	t.Run("Test Dequeue", goqueue_tests.TestDequeue(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Length
	} {
		return new(t, configuration)
	}))
	t.Run("Test Dequeue Multiple", goqueue_tests.TestDequeueMultiple(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
	} {
		return new(t, configuration)
	}))
	t.Run("Test Flush", goqueue_tests.TestFlush(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
	} {
		return new(t, configuration)
	}))
	t.Run("Test Enqueue", infinite_tests.TestEnqueue(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Length
	} {
		return new(t, configuration)
	}))
	t.Run("Test Enqueue Multiple", infinite_tests.TestEnqueueMultiple(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Length
	} {
		return new(t, configuration)
	}))
	// t.Run("Test Peek", goqueue_tests.TestPeek(t, func(size int) interface {
	// 	goqueue.Owner
	// 	goqueue.Enqueuer
	// 	goqueue.Dequeuer
	// 	goqueue.Peeker
	// } {
	// 	return new(t, configuration)
	// }))
	// t.Run("Test Peek From Head", goqueue_tests.TestPeekFromHead(t, func(size int) interface {
	// 	goqueue.Owner
	// 	goqueue.Enqueuer
	// 	goqueue.Dequeuer
	// 	goqueue.Peeker
	// } {
	// 	return new(t, configuration)
	// }))
	t.Run("Test Length", goqueue_tests.TestLength(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Length
	} {
		return new(t, configuration)
	}))
	t.Run("Test Queue (goqueue)", goqueue_tests.TestQueue(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Length
	} {
		return new(t, configuration)
	}))
	t.Run("Test Queue (infinite)", infinite_tests.TestQueue(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Length
	} {
		return new(t, configuration)
	}))
	t.Run("Test Asynchronous", goqueue_tests.TestAsync(t, func(size int) interface {
		goqueue.Owner
		goqueue.Enqueuer
		goqueue.Dequeuer
		goqueue.Length
	} {
		return new(t, configuration)
	}))
	//TODO: test error handler
}
