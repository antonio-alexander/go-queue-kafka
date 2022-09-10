package kafka

import (
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	DefaultTopicIn     string        = "queue.in"
	DefaultTopicOut    string        = "queue.out"
	DefaultQueueSize   int           = 10000
	DefaultCommitRate  time.Duration = time.Minute
	DefaultEnqueueRate time.Duration = time.Millisecond
)

var DefaultBrokers = []string{"localhost:9092"}

const (
	EnvNameKafkaBrokers   string = "KAFKA_BROKERS"
	EnvNameKafkaClientId  string = "KAFKA_CLIENT_ID"
	EnvNameKafkaGroupId   string = "KAFKA_GROUP_ID"
	EnvNameKafkaEnableLog string = "KAFKA_ENABLE_LOG"
	EnvNameKafkaQueueSize string = "KAFKA_QUEUE_SIZE"
	EnvNameKafkaTopicIn   string = "KAFKA_TOPIC_IN"
	EnvNameKafkaTopicOut  string = "KAFKA_TOPIC_OUT"
	EnvNameCommitRate     string = "KAFKA_COMMIT_RATE"
	EnvNameEnqueueRate    string = "KAFKA_ENQUEUE_RATE"
)

const (
	UnsupportedTypef               string = "unsupported type: %T"
	NoBrokersConfigured            string = "no brokers configured"
	NoClientIdConfigured           string = "no client id configured"
	NoGroupIdConfigured            string = "no group id configured"
	CommitRateLessThanOrEqualZero  string = "commit rate less than or equal to zero"
	EnqueueRateLessThanOrEqualZero string = "enqueue rate less than or equal to zero"
)

var (
	ErrNoBrokersConfigured            = errors.New(NoBrokersConfigured)
	ErrNoClientIdConfigured           = errors.New(NoClientIdConfigured)
	ErrNoGroupIdConfigured            = errors.New(NoGroupIdConfigured)
	ErrCommitRateLessThanOrEqualZero  = errors.New(CommitRateLessThanOrEqualZero)
	ErrEnqueueRateLessThanOrEqualZero = errors.New(EnqueueRateLessThanOrEqualZero)
)

type Configuration struct {
	Brokers       []string      `json:"brokers"`
	ClientId      string        `json:"client_id"`
	GroupId       string        `json:"group_id"`
	EnableLog     bool          `json:"enable_log"`
	ConsumerGroup bool          `json:"consumer_group"`
	QueueSize     int           `json:"queue_size"`
	TopicIn       string        `json:"topic_in"`
	TopicOut      string        `json:"topic_out"`
	CommitRate    time.Duration `json:"commit_rate"`
	EnqueueRate   time.Duration `json:"enqueue_rate"`
}

func (c *Configuration) Default() {
	c.ClientId = uuid.Must(uuid.NewRandom()).String()
	c.GroupId = uuid.Must(uuid.NewRandom()).String()
	c.EnableLog = false
	c.QueueSize = DefaultQueueSize
	c.TopicIn = DefaultTopicIn
	c.TopicOut = DefaultTopicOut
	c.CommitRate = DefaultCommitRate
	c.EnqueueRate = DefaultEnqueueRate
}

func (c *Configuration) FromEnv(envs map[string]string) {
	if queueSize, ok := envs[EnvNameKafkaQueueSize]; ok {
		c.QueueSize, _ = strconv.Atoi(queueSize)
	}
	if topicIn, ok := envs[EnvNameKafkaTopicIn]; ok {
		c.TopicIn = topicIn
	}
	if topicOut, ok := envs[EnvNameKafkaTopicOut]; ok {
		c.TopicOut = topicOut
	}
	if brokers, ok := envs[EnvNameKafkaBrokers]; ok && brokers != "" {
		c.Brokers = strings.Split(brokers, ",")
	}
	if clientId, ok := envs[EnvNameKafkaClientId]; ok && clientId != "" {
		c.ClientId = clientId
	}
	if groupId, ok := envs[EnvNameKafkaClientId]; ok && groupId != "" {
		c.GroupId = groupId
	}
	if s, ok := envs[EnvNameKafkaEnableLog]; ok && s != "" {
		c.EnableLog, _ = strconv.ParseBool(s)
	}
	if s, ok := envs[EnvNameCommitRate]; ok {
		c.CommitRate, _ = time.ParseDuration(s)
	}
	if s, ok := envs[EnvNameEnqueueRate]; ok {
		c.EnqueueRate, _ = time.ParseDuration(s)
	}
}

func (c *Configuration) Validate() error {
	if len(c.Brokers) == 0 {
		return ErrNoBrokersConfigured
	}
	if c.GroupId == "" {
		return ErrNoGroupIdConfigured
	}
	if c.ClientId == "" {
		return ErrNoClientIdConfigured
	}
	if c.CommitRate <= 0 {
		return ErrCommitRateLessThanOrEqualZero
	}
	if c.EnqueueRate <= 0 {
		return ErrEnqueueRateLessThanOrEqualZero
	}
	return nil
}

func (c *Configuration) ToKafka() ([]string, *sarama.Config) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = c.ClientId
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.ChannelBufferSize = 1024
	kafkaConfig.Consumer.Return.Errors = true
	return c.Brokers, kafkaConfig
}
