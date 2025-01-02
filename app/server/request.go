package server

import (
	"bytes"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/encoding/kafka"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/codecrafters-io/kafka-starter-go/app/support"
)

type RequestHeadersV0 struct {
	CorrelationId int32 `kafka:"0"`
}

type RequestHeadersV1 struct {
	RequestHeadersV0
	ClientId string `kafka:"1"`
}

type RequestHeadersV2 struct {
	RequestHeadersV1
	TaggedFields []protocol.TaggedField `kafka:"2"`
}

type Request struct {
	MessageSize int32
	ApiVersion  struct {
		Key     support.ApiKey `kafka:"0"`
		Version int16          `kafka:"1"`
	}
	headers any
	Body    io.Reader
}

func (r *Request) CorrelationId() int32 {
	switch h := r.headers.(type) {
	case RequestHeadersV0:
		return h.CorrelationId
	case RequestHeadersV1:
		return h.CorrelationId
	case RequestHeadersV2:
		return h.CorrelationId
	default:
		return 0
	}
}

func (r *Request) ClientId() string {
	switch h := r.headers.(type) {
	case RequestHeadersV1:
		return h.ClientId
	case RequestHeadersV2:
		return h.ClientId
	default:
		return ""
	}
}

func (r *Request) TaggedFields() []protocol.TaggedField {
	switch h := r.headers.(type) {
	case RequestHeadersV2:
		return h.TaggedFields
	default:
		return []protocol.TaggedField{}
	}
}

func ParseRequest(reader io.Reader) (request *Request, err error) {
	request = new(Request)

	if err = kafka.NewDecoder(reader).Decode(&request.MessageSize); err != nil {
		return nil, err
	}

	var messageReader io.Reader

	if messageReader, err = readMessage(reader, request.MessageSize); err != nil {
		return nil, err
	}

	decoder := kafka.NewDecoder(messageReader)

	if err = decoder.Decode(&request.ApiVersion); err != nil {
		return nil, err
	}

	headers := new(RequestHeadersV0)

	request.headers = headers
	// if request.CorrelationId, err = support.ReadInt32(reader); err != nil {
	// 	return nil, err
	// }

	// if request.ClientId, err = support.ReadString(dataReader); err != nil {
	// 	return nil, err
	// }

	// if _, err = support.ReadByte(dataReader); err != nil {
	// 	return nil, err
	// }

	request.Body = messageReader

	return request, nil
}

func readMessage(reader io.Reader, messageSize int32) (bytesReader io.Reader, err error) {
	message := make([]byte, messageSize)

	if _, err = reader.Read(message); err != nil {
		return nil, err
	}

	return bytes.NewReader(message), nil
}
