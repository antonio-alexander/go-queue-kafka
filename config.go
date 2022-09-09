package kafka

import (
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	DefaultTopicIn      string = "queue.in"
	DefaultTopicOut     string = "queue.out"
	DefaultQueueSize    int    = 10000
	ErrUnsupportedTypef string = "unsupported type: %T"
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
)

const (
	NoBrokersConfigured  string = "no brokers configured"
	NoClientIdConfigured string = "no client id configured"
	NoGroupIdConfigured  string = "no group id configured"
)

var (
	ErrNoBrokersConfigured  = errors.New(NoBrokersConfigured)
	ErrNoClientIdConfigured = errors.New(NoClientIdConfigured)
	ErrNoGroupIdConfigured  = errors.New(NoGroupIdConfigured)
)

type Configuration struct {
	Brokers       []string `json:"brokers"`
	ClientId      string   `json:"client_id"`
	GroupId       string   `json:"group_id"`
	EnableLog     bool     `json:"enable_log"`
	ConsumerGroup bool     `json:"consumer_group"`
	QueueSize     int      `json:"queue_size"`
	TopicIn       string   `json:"topic_in"`
	TopicOut      string   `json:"topic_out"`
}

func (c *Configuration) Default() {
	c.ClientId = uuid.Must(uuid.NewRandom()).String()
	c.GroupId = uuid.Must(uuid.NewRandom()).String()
	c.EnableLog = false
	c.QueueSize = DefaultQueueSize
	c.TopicIn = DefaultTopicIn
	c.TopicOut = DefaultTopicOut
}

func (c *Configuration) FromEnv(envs map[string]string) {
	if queueSize, ok := envs["KAFKA_QUEUE_SIZE"]; ok {
		c.QueueSize, _ = strconv.Atoi(queueSize)
	}
	if topic, ok := envs["KAFKA_TOPIC_IN"]; ok {
		c.TopicIn = topic
	}
	if topic, ok := envs["KAFKA_TOPIC_OUT"]; ok {
		c.TopicOut = topic
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
	return nil
}
