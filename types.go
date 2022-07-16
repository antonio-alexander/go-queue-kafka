package kafka

const (
	defaultKafkaTopic    string = ""
	defaultKafkaHost     string = ""
	defaultKafkaPort     string = ""
	defaultKafkaClientID string = ""
	defaultQueueSize     int    = 10000
)

type Configuration struct {
	KafkaTopic     string
	KafkaHost      string
	KafkaPort      string
	KafkaClientID  string
	KafkaEnableLog bool
	QueueSize      int
}

type Owner interface {
	Close()
	Start(config *Configuration) (err error)
	Stop()
}

type Wrapper struct {
	Type  string `json:"type,omitempty"`
	Bytes []byte `json:"bytes"`
}
