package kafka

import (
	"bytes"
	"fmt"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka/support"
)

type Response struct {
	MessageSize   int32
	CorrelationId int32
	Body          *bytes.Buffer
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

	fmt.Println(r.Body.Bytes())

	if _, err = writer.Write(r.Body.Bytes()); err != nil {
		return err
	}

	return nil
}
