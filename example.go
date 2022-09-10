package kafka

import (
	"encoding"
	"encoding/json"

	goqueue "github.com/antonio-alexander/go-queue"

	"github.com/pkg/errors"
)

const typeExample = "Example"

type Wrapper struct {
	Type  string `json:"type"`
	Bytes []byte `json:"bytes"`
}

func (w *Wrapper) MarshalBinary() ([]byte, error) {
	return json.Marshal(w)
}

func (w *Wrapper) UnmarshalBinary(bytes []byte) error {
	return json.Unmarshal(bytes, w)
}

func (w *Wrapper) ToWrapper(item interface {
	encoding.BinaryMarshaler
	Type() string
}) error {
	bytes, err := item.MarshalBinary()
	if err != nil {
		return err
	}
	w.Bytes = bytes
	w.Type = item.Type()
	return nil
}

func (w *Wrapper) FromWrapper() (interface{}, error) {
	switch w.Type {
	case typeExample:
		example := &Example{}
		if err := example.UnmarshalBinary(w.Bytes); err != nil {
			return nil, err
		}
		return example, nil
	}
	return nil, errors.New("unsupported type")
}

type Example struct {
	goqueue.Example
}

func (e *Example) Type() string {
	return typeExample
}

func (e *Example) MarshalBinary() ([]byte, error) {
	return json.Marshal(e)
}

func (e *Example) UnmarshalBinary(bytes []byte) error {
	return json.Unmarshal(bytes, e)
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

func ExampleClose(queue goqueue.Owner) []*Wrapper {
	items := queue.Close()
	return WrapperConvertMultiple(items)
}

func ExampleEnqueueMultiple(queue goqueue.Enqueuer, values []*Wrapper) ([]*Wrapper, bool) {
	items := make([]interface{}, 0, len(values))
	for _, value := range values {
		items = append(items, value)
	}
	itemsRemaining, overflow := queue.EnqueueMultiple(items)
	if !overflow {
		return nil, false
	}
	return WrapperConvertMultiple(itemsRemaining), true
}

func ExamplePeek(queue goqueue.Peeker) []*Wrapper {
	items := queue.Peek()
	return WrapperConvertMultiple(items)
}

func ExamplePeekHead(queue goqueue.Peeker) (*Wrapper, bool) {
	item, underflow := queue.PeekHead()
	if underflow {
		return nil, true
	}
	return WrapperConvertSingle(item), false
}

func ExamplePeekFromHead(queue goqueue.Peeker, n int) []*Wrapper {
	items := queue.PeekFromHead(n)
	return WrapperConvertMultiple(items)
}

func WrapperDequeue(queue goqueue.Dequeuer) (*Wrapper, bool) {
	item, underflow := queue.Dequeue()
	if underflow {
		return nil, true
	}
	return WrapperConvertSingle(item), false
}

func WrapperDequeueMultiple(queue goqueue.Dequeuer, n int) []*Wrapper {
	items := queue.DequeueMultiple(n)
	return WrapperConvertMultiple(items)
}

func WrapperFlush(queue goqueue.Dequeuer) []*Wrapper {
	items := queue.Flush()
	return WrapperConvertMultiple(items)
}
