package kafka

import (
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

func createKafkaClient(c Configuration) (sarama.Client, error) {
	//create configuration, create producer, and then set the logger
	config := sarama.NewConfig()
	config.ClientID = c.KafkaClientID
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.ChannelBufferSize = 1024
	config.Consumer.Return.Errors = true
	client, err := sarama.NewClient([]string{fmt.Sprintf("%s:%s", c.KafkaHost, c.KafkaPort)}, config)
	if err != nil {
		return nil, err
	}
	if c.KafkaEnableLog {
		sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	}
	return client, nil
}
