package server

import (
	"bytes"
	"fmt"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/encoding/kafka"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/codecrafters-io/kafka-starter-go/app/support"
)

type RequestHeaders struct {
	CorrelationId int32                  `kafka:"0,minVersion=0"`
	ClientId      string                 `kafka:"1,minVersion=1"`
	TaggedFields  []protocol.TaggedField `kafka:"2,minVersion=2,compact"`
}

type Request struct {
	MessageSize int32
	ApiVersion  struct {
		Key     support.ApiKey `kafka:"0"`
		Version int16          `kafka:"1"`
	}
	Headers RequestHeaders
	Body    io.Reader
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

	if err = decoder.DecodeWithOpts(&request.Headers, &kafka.DecoderOpts{
		Version: 2,
	}); err != nil {
		return nil, err
	}

	request.Body = messageReader

	if b, ok := messageReader.(*bytes.Buffer); ok {
		fmt.Println(b.Bytes())
	}

	return request, nil
}

func readMessage(reader io.Reader, messageSize int32) (bytesReader io.Reader, err error) {
	message := make([]byte, messageSize)

	if _, err = reader.Read(message); err != nil {
		return nil, err
	}

	return bytes.NewBuffer(message), nil
}
