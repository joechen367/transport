// Package proto defines the protobuf codec. Importing this package will
// register the codec.
package proto

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/joechen367/transport/encoding"
	"google.golang.org/protobuf/proto"
)

// Name is the name registered for the proto compressor.
const Name = "proto"

func init() {
	fmt.Println("proto")
	encoding.RegisterCodec(codec{})
}

// codec is a Codec implementation with protobuf. It is the default codec for Transport.
type codec struct{}

func (codec) Marshal(v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}

func (codec) Unmarshal(data []byte, v interface{}) error {
	pm, err := getProtoMessage(v)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, pm)
}

func (codec) Name() string {
	// fmt.Println("获取" + Name)
	return Name
}

func getProtoMessage(v interface{}) (proto.Message, error) {
	if msg, ok := v.(proto.Message); ok {
		return msg, nil
	}
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr {
		return nil, errors.New("not proto message")
	}

	val = val.Elem()
	return getProtoMessage(val.Interface())
}
