package kafka

import (
	"encoding"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
)

var _ interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
} = &Wrapper{}

type Wrapper struct {
	Type  string `json:"type,omitempty"`
	Bytes []byte `json:"bytes"`
}

func (w *Wrapper) MarshalBinary() ([]byte, error) {
	return json.Marshal(w)
}

func (w *Wrapper) UnmarshalBinary(bytes []byte) error {
	return json.Unmarshal(bytes, w)
}

func (w *Wrapper) ToWrapper(item Wrappable) error {
	bytes, err := item.MarshalBinary()
	if err != nil {
		return err
	}
	w.Bytes = bytes
	w.Type = item.Type()
	return nil
}

func (w *Wrapper) FromWrapper() (interface{}, error) {
	switch strings.ToLower(w.Type) {
	case "example":
		example := &Example{}
		if err := example.UnmarshalBinary(w.Bytes); err != nil {
			return nil, err
		}
		return example, nil
	}
	return nil, errors.New("unsupported type")
}
