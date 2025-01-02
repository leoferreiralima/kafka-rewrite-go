package server

import (
	"bytes"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/support"
)

type Response struct {
	MessageSize   int32
	CorrelationId int32
	Body          *bytes.Buffer
	TagBuffer     byte
}

func NewResponse() Response {
	return Response{
		MessageSize: 0,
		Body:        new(bytes.Buffer),
	}
}

func (r *Response) Write(writer io.Writer) (err error) {
	r.MessageSize = int32(r.Body.Len()) + 4

	if err = support.WriteInt32(writer, r.MessageSize); err != nil {
		return err
	}

	if err = support.WriteInt32(writer, r.CorrelationId); err != nil {
		return err
	}

	if _, err = writer.Write(r.Body.Bytes()); err != nil {
		return err
	}

	return nil
}
