package kafka_test

import (
	"os"
	"strings"
	"testing"

	kafka "github.com/antonio-alexander/go-queue-kafka"
	pb "github.com/antonio-alexander/go-queue-kafka/protos"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var (
	kafkaTopic    string = "meta.queue"
	kafkaHost     string = "localhost"
	kafkaPort     string = "9092"
	kafkaClientID string = uuid.Must(uuid.NewRandom()).String()
)

func init() {
	envs := make(map[string]string)
	for _, s := range os.Environ() {
		if s := strings.Split(s, "="); len(s) > 1 {
			envs[s[0]] = strings.Join(s[1:], "=")
		}
	}
	if topic, ok := envs["KAFKA_TOPIC"]; ok {
		kafkaTopic = topic
	}
	if host, ok := envs["KAFKA_HOST"]; ok {
		kafkaHost = host
	}
	if port, ok := envs["KAFKA_PORT"]; ok {
		kafkaPort = port
	}
	if clientID, ok := envs["KAFKA_CLIENT_ID"]; ok {
		kafkaClientID = clientID
	}
}

func TestKafka(t *testing.T) {
	k := kafka.New()
	assert.NotNil(t, k)
	err := k.Start(&kafka.Configuration{
		KafkaTopic:     kafkaTopic,
		KafkaHost:      kafkaHost,
		KafkaPort:      kafkaPort,
		KafkaClientID:  kafkaClientID,
		KafkaEnableLog: true,
		QueueSize:      0,
	})
	assert.Nil(t, err)
	wrapper := &pb.Wrapper{
		Type: "type",
	}
	bytes, err := proto.Marshal(wrapper)
	assert.Nil(t, err)
	assert.NotEmpty(t, bytes)
	//attempt to enqueue
	overflow := k.Enqueue(bytes)
	assert.False(t, overflow)
	//attempt to dequeue
	item, underflow := k.Dequeue()
	assert.False(t, underflow)
	bytes, ok := item.([]byte)
	if assert.True(t, ok) {
		wrapperRead := &pb.Wrapper{}
		err = proto.Unmarshal(bytes, wrapperRead)
		assert.Nil(t, err)
		assert.Equal(t, wrapper.Type, wrapperRead.Type)
	}
	k.Stop()
	k.Close()
}
