package kafka

import (
	"io"
)

type Request struct {
	MessageSize   int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	Data          []byte
}

func ParseRequest(reader io.Reader) (request *Request, err error) {
	request = new(Request)

	if request.MessageSize, err = ReadInt32(reader); err != nil {
		return nil, err
	}

	if request.ApiKey, err = ReadInt16(reader); err != nil {
		return nil, err
	}

	if request.ApiVersion, err = ReadInt16(reader); err != nil {
		return nil, err
	}

	if request.CorrelationId, err = ReadInt32(reader); err != nil {
		return nil, err
	}

	remaningBytesToRead := request.MessageSize - 8

	request.Data = make([]byte, remaningBytesToRead)

	if _, err = reader.Read(request.Data); err != nil {
		return nil, err
	}

	return request, nil
}
