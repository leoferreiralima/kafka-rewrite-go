package kafka

import (
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka/support"
)

type Request struct {
	MessageSize   int32
	ApiKey        support.ApiKey
	ApiVersion    int16
	CorrelationId int32
	Data          []byte
}

func ParseRequest(reader io.Reader) (request *Request, err error) {
	request = new(Request)

	if request.MessageSize, err = support.ReadInt32(reader); err != nil {
		return nil, err
	}

	apiKey, err := support.ReadInt16(reader)
	if err != nil {
		return nil, err
	}

	request.ApiKey = support.ApiKey(apiKey)

	if request.ApiVersion, err = support.ReadInt16(reader); err != nil {
		return nil, err
	}

	if request.CorrelationId, err = support.ReadInt32(reader); err != nil {
		return nil, err
	}

	remaningBytesToRead := request.MessageSize - 8

	request.Data = make([]byte, remaningBytesToRead)

	if _, err = reader.Read(request.Data); err != nil {
		return nil, err
	}

	return request, nil
}
