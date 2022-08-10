package kafka

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	goqueue "github.com/antonio-alexander/go-queue"

	"github.com/Shopify/sarama"
)

func CreateKafkaClient(c Configuration) (sarama.Client, error) {
	//create configuration, create producer, and then set the logger
	config := sarama.NewConfig()
	config.ClientID = c.ClientID
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.ChannelBufferSize = 1024
	config.Consumer.Return.Errors = true
	client, err := sarama.NewClient([]string{fmt.Sprintf("%s:%s", c.Host, c.Port)}, config)
	if err != nil {
		return nil, err
	}
	if c.EnableLog {
		sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	}
	return client, nil
}

func MustEnqueue(queue goqueue.Enqueuer, item interface{}) {
	if overflow := queue.Enqueue(item); !overflow {
		return
	}
	tEnqueue := time.NewTicker(time.Second)
	defer tEnqueue.Stop()
	for {
		<-tEnqueue.C
		if overflow := queue.Enqueue(item); !overflow {
			return
		}
	}
}

func MustDequeue(queue interface{}) interface{} {
	switch queue := queue.(type) {
	default:
		return nil
	case interface {
		goqueue.Dequeuer
		goqueue.Event
	}:
		if item, overflow := queue.Dequeue(); !overflow {
			return item
		}
		signalIn := queue.GetSignalIn()
		for {
			<-signalIn
			if item, overflow := queue.Dequeue(); !overflow {
				return item
			}
		}
	case goqueue.Dequeuer:
		if item, overflow := queue.Dequeue(); !overflow {
			return item
		}
		tDequeue := time.NewTicker(time.Millisecond)
		defer tDequeue.Stop()
		for {
			<-tDequeue.C
			if item, overflow := queue.Dequeue(); !overflow {
				return item
			}
		}
	}
}

//ConfigFromEnv can be used to generate a configuration pointer
// from a list of environments, it'll set the default configuraton
// as well
func ConfigFromEnv(envs map[string]string) *Configuration {
	c := &Configuration{
		Host:      DefaultHost,
		Port:      DefaultPort,
		EnableLog: DefaultEnableLog,
		QueueSize: DefaultQueueSize,
		TopicIn:   DefaultTopicIn,
		TopicOut:  DefaultTopicOut,
	}
	if host, ok := envs["KAFKA_HOST"]; ok {
		c.Host = host
	}
	if port, ok := envs["KAFKA_PORT"]; ok {
		c.Port = port
	}
	if enableLog, ok := envs["KAFKA_ENABLE_LOG"]; ok {
		c.EnableLog, _ = strconv.ParseBool(enableLog)
	}
	if queueSize, ok := envs["KAFKA_QUEUE_SIZE"]; ok {
		c.QueueSize, _ = strconv.Atoi(queueSize)
	}
	if clientID, ok := envs["KAFKA_CLIENT_ID"]; ok {
		c.ClientID = clientID
	}
	if topic, ok := envs["KAFKA_TOPIC_IN"]; ok {
		c.TopicIn = topic
	}
	if topic, ok := envs["KAFKA_TOPIC_OUT"]; ok {
		c.TopicOut = topic
	}
	return c
}
