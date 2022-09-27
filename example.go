package kafka

import (
	pb "github.com/antonio-alexander/go-queue-kafka/protos"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

var nameExample = string(proto.MessageName(&pb.Example{}))

type Wrapper struct {
	*pb.Wrapper
}

func (w *Wrapper) MarshalBinary() ([]byte, error) {
	return proto.Marshal(w)
}

func (w *Wrapper) UnmarshalBinary(bytes []byte) error {
	if w.Wrapper == nil {
		w.Wrapper = &pb.Wrapper{}
	}
	return proto.Unmarshal(bytes, w)
}

func (w *Wrapper) ToWrapper(message proto.Message) error {
	bytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	w.Bytes = bytes
	w.Type = string(proto.MessageName(message))
	return nil
}

func (w *Wrapper) FromWrapper() (interface{}, error) {
	switch w.Type {
	case nameExample:
		example := &Example{&pb.Example{}}
		if err := example.UnmarshalBinary(w.Bytes); err != nil {
			return nil, err
		}
		return example, nil
	}
	return nil, errors.New("unsupported type")
}

type Example struct {
	*pb.Example
}

func (e *Example) MarshalBinary() ([]byte, error) {
	return proto.Marshal(e)
}

func (e *Example) UnmarshalBinary(bytes []byte) error {
	if e.Example == nil {
		e.Example = &pb.Example{}
	}
	return proto.Unmarshal(bytes, e)
}

func WrapperConvertSingle(item interface{}) *Wrapper {
	switch v := item.(type) {
	default:
		return nil
	case *Wrapper:
		return v
	case []byte:
		wrapper := &Wrapper{}
		if err := wrapper.UnmarshalBinary(v); err != nil {
			return nil
		}
		return wrapper
	}
}

func WrapperConvertMultiple(items []interface{}) []*Wrapper {
	wrappers := make([]*Wrapper, 0, len(items))
	for _, item := range items {
		value := WrapperConvertSingle(item)
		if value == nil {
			continue
		}
		wrappers = append(wrappers, value)
	}
	return wrappers
}
