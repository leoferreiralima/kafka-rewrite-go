package kafka

import (
	"io"
)

type Response struct {
	MessageSize   int32
	CorrelationId int32
}

func NewResponse() Response {
	return Response{
		MessageSize: 0,
	}
}

func (r *Response) Write(writer io.Writer) (err error) {
	if err = WriteInt32(writer, r.MessageSize); err != nil {
		return err
	}

	if err = WriteInt32(writer, r.CorrelationId); err != nil {
		return err
	}

	return nil
}
