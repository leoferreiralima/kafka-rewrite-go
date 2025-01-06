package server

import (
	"bytes"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

type ResponseWriter interface {
	Write(p []byte) (n int, err error)
}

type responseHeader struct {
	CorrelationId int32                  `kafka:"0"`
	TaggedFields  []protocol.TaggedField `kafka:"1,minVersion=1,compact,nilable"`
}

type response struct {
	conn    *conn
	req     *Request
	headers responseHeader
	buffer  *bytes.Buffer
}

func (r *response) Write(p []byte) (n int, err error) {
	return r.buffer.Write(p)
}

func (r *response) messageSize() int32 {
	return int32(r.buffer.Len())
}
