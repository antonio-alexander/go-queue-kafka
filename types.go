package kafka

import (
	"encoding"
)

type ErrorHandlerFx func(error)

type Owner interface {
	Initialize(config *Configuration) (err error)
	Shutdown()
}

type Wrappable interface {
	encoding.BinaryMarshaler
	Type() string
}
