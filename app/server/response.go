package server

import (
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
)

type ResponseWriter interface {
	Write(p []byte) (n int, err error)
}

type responseHeader struct {
	CorrelationId int32                  `kafka:"0"`
	TaggedFields  []handlers.TaggedField `kafka:"1,minVersion=1,compact,nilable"`
}

type response struct {
	header  responseHeader
	writer  io.Writer
	written int
}

func NewResponseWriter(writer io.Writer) ResponseWriter {
	response := &response{
		writer: writer,
	}

	return response
}

func (r *response) Write(p []byte) (n int, err error) {
	r.written += len(p)
	return r.writer.Write(p)
}

func (r *response) messageSize() int32 {
	return int32(r.written)
}
