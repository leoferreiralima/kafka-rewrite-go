package kafka

import (
	"bytes"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka/support"
)

type Request struct {
	MessageSize   int32
	ApiKey        support.ApiKey
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
	Body          io.Reader
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

	data := make([]byte, remaningBytesToRead)

	if _, err = reader.Read(data); err != nil {
		return nil, err
	}

	dataReader := bytes.NewReader(data)

	if request.ClientId, err = support.ReadString(dataReader); err != nil {
		return nil, err
	}

	if _, err = support.ReadByte(dataReader); err != nil {
		return nil, err
	}

	request.Body = dataReader

	return request, nil
}
