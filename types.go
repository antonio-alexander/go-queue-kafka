package kafka

const (
	DefaultTopicIn      string = "queue.in"
	DefaultTopicOut     string = "queue.out"
	DefaultHost         string = "localhost"
	DefaultPort         string = "9092"
	DefaultEnableLog    bool   = false
	DefaultQueueSize    int    = 10000
	ErrUnsupportedTypef string = "unsupported type: %T"
)

type Configuration struct {
	Host      string
	Port      string
	ClientID  string
	GroupID   string
	EnableLog bool
	QueueSize int
	TopicIn   string
	TopicOut  string
}

type Owner interface {
	Initialize(config *Configuration) (err error)
	Shutdown() []interface{}
}

type ErrorHandlerFx func(error)

type Wrapper struct {
	Type  string `json:"type,omitempty"`
	Bytes []byte `json:"bytes"`
}
