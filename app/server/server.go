package server

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"sync"

	"github.com/codecrafters-io/kafka-starter-go/app/encoding/kafka"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

type HandlerFunc func(ResponseWriter, *Request) error

type HandlerRequestOpts struct {
	Version int16
}

type HandlerResponseOpts struct {
	Version int16
}

type HandlerOpts struct {
	Request  HandlerRequestOpts
	Response HandlerResponseOpts
}

type handlerState struct {
	opts        HandlerOpts
	handlerFunc HandlerFunc
}

type KafkaApiKey struct {
	ApiKey     protocol.ApiKey
	MinVersion protocol.ApiVersion
	MaxVersion protocol.ApiVersion
}

type KafkaServer struct {
	mutex    sync.RWMutex
	logger   *log.Logger
	handlers map[KafkaApiKey]handlerState
}

type conn struct {
	server     *KafkaServer
	connection net.Conn
}

func NewKafkaServer() *KafkaServer {
	return &KafkaServer{
		logger:   log.New(os.Stdout, "kafka-server:", log.LstdFlags|log.LUTC|log.Lmsgprefix|log.Lshortfile),
		handlers: make(map[KafkaApiKey]handlerState),
	}
}

func (ks *KafkaServer) HandlerFunc(
	apiKey KafkaApiKey,
	handler HandlerFunc,
	opts *HandlerOpts,
) {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	for handlerKey := range ks.handlers {
		if apiKey.ApiKey != handlerKey.ApiKey {
			continue
		}

		hasConflictWithMinVersion := apiKey.MinVersion >= handlerKey.MinVersion && apiKey.MinVersion <= handlerKey.MaxVersion
		hasConflictWithMaxVersion := apiKey.MaxVersion >= handlerKey.MinVersion && apiKey.MaxVersion <= handlerKey.MaxVersion

		if hasConflictWithMinVersion || hasConflictWithMaxVersion {
			ks.logger.Panicf(
				"api[key=%d,minVersion=%d,maxVersion=%version] has conflict with already registred api[key=%d,minVersion=%d,maxVersion=%version]",
				apiKey.ApiKey, apiKey.MinVersion, apiKey.MaxVersion,
				handlerKey.ApiKey, handlerKey.MinVersion, handlerKey.MaxVersion,
			)
		}
	}

	if opts == nil {
		opts = &HandlerOpts{
			Request: HandlerRequestOpts{
				Version: 2,
			},
			Response: HandlerResponseOpts{
				Version: 0,
			},
		}
	}
	ks.handlers[apiKey] = handlerState{
		opts:        *opts,
		handlerFunc: handler,
	}
}

func (ks *KafkaServer) ListenAndServe(addr string) error {
	listener, err := net.Listen("tcp", addr)

	if err != nil {
		return err
	}

	defer listener.Close()

	return ks.serve(listener)
}

func (ks *KafkaServer) serve(listener net.Listener) error {
	for {
		connection, err := listener.Accept()

		if err != nil {
			return err
		}

		conn := ks.newConn(connection)
		go conn.serve()
	}

}

func (ks *KafkaServer) handleRequest(res *response, req *Request) {
	handlerState, found := ks.findHandler(req)

	if !found {
		ks.handleError(res, protocol.UnsupportedVersion)
		return
	}

	opts := handlerState.opts
	handler := handlerState.handlerFunc

	if err := ks.writeResponseHeaders(res, int(opts.Response.Version)); err != nil {
		ks.logger.Printf("Couldn't encode response headers: %s\n%v", fmt.Sprint(res.headers), err)
		ks.handleError(res, protocol.UnknownServerError)
	}

	if err := handler(res, req); err != nil {
		ks.logger.Printf("Couldn't handle request:%v", err)
		ks.handleError(res, protocol.UnknownServerError)
	}

	if err := ks.sendResponse(res); err != nil {
		ks.logger.Printf("Couldn't send response:%v", err)
		ks.handleError(res, protocol.UnknownServerError)
	}
}

func (ks *KafkaServer) findHandler(req *Request) (handler *handlerState, exists bool) {
	apiVersion := req.ApiVersion
	for key, handler := range ks.handlers {

		if apiVersion.Key == key.ApiKey && apiVersion.Version >= key.MinVersion && apiVersion.Version <= key.MaxVersion {
			return &handler, true
		}
	}

	return nil, false
}

func (ks *KafkaServer) handleError(res *response, errorCode protocol.ErrorCode) {
	handlerState, found := ks.findHandler(res.req)
	version := 0
	if found {
		version = int(handlerState.opts.Response.Version)
	}

	if err := ks.writeResponseHeaders(res, version); err != nil { // TODO fix version here
		ks.logger.Panicf("Couldn't encode response headers: %s\n%v", fmt.Sprint(res.headers), err)
	}

	if err := kafka.NewEncoder(res).Encode(errorCode); err != nil {
		ks.logger.Panicf("Couldn't encode response errorCode %d:\n%v", errorCode, err)
	}

	if err := ks.sendResponse(res); err != nil {
		ks.logger.Panicf("Couldn't send response:%v", err)
	}
}

func (ks *KafkaServer) writeResponseHeaders(res *response, version int) (err error) {
	if err = kafka.NewEncoder(res).EncodeWithOpts(res.headers, &kafka.EncoderOpts{
		Version: version,
	}); err != nil {
		return err
	}

	return nil
}

func (ks *KafkaServer) sendResponse(res *response) (err error) {
	connection := res.conn.connection
	if err = kafka.NewEncoder(connection).Encode(res.messageSize()); err != nil {
		return err
	}

	if _, err = connection.Write(res.buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (ks *KafkaServer) newConn(connection net.Conn) *conn {
	return &conn{
		ks,
		connection,
	}
}

func (c *conn) serve() {
	defer func() {
		if err := recover(); err != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]

			c.server.logger.Printf("panic: %v\n%s", err, buf)
		}
		c.close()
	}()

	for {
		response, err := c.readRequest()

		if err != nil {
			c.server.handleError(response, protocol.UnknownServerError)
		}

		c.server.handleRequest(response, response.req)
	}
}

func (c *conn) close() {
	c.connection.Close()
}

func (c *conn) readRequest() (res *response, err error) {
	request, err := ParseRequest(c.connection)

	if err != nil {
		return nil, err
	}

	res = &response{
		conn:   c,
		req:    request,
		buffer: new(bytes.Buffer),
	}

	res.headers.CorrelationId = request.Headers.CorrelationId

	return res, nil
}

// func HandleConnection(conn net.Conn) {
// 	defer conn.Close()

// 	for {
// 		request, err := ParseRequest(conn)

// 		if err != nil {
// 			if err == io.EOF {
// 				fmt.Println("Connection ended")
// 				break
// 			}
// 			panic(err)
// 		}

// 		fmt.Printf("Request ApiKey: %d, ApiVersion: %d\n", request.ApiVersion.Key, request.ApiVersion.Version)
// 		writer := bufio.NewWriter(conn)
// 		err = requestHandler(request, writer)

// 		if err != nil {
// 			fmt.Println("Error on handle request: ", err.Error())
// 			break
// 		}

// 		if err = writer.Flush(); err != nil {
// 			fmt.Println("Error on handle request: ", err.Error())
// 			break
// 		}
// 	}
// }

// func requestHandler(request *Request, writer io.Writer) (err error) {
// 	buffer := new(bytes.Buffer)
// 	response := &response{
// 		writer: buffer,
// 	}

// 	response.header.CorrelationId = request.Headers.CorrelationId

// 	responseHeaderVersion := 0

// 	if request.ApiVersion.Key == handlers.DescribeTopicPartitions {
// 		responseHeaderVersion = 1
// 	}

// 	if err = kafka.NewEncoder(response).EncodeWithOpts(response.header, &kafka.EncoderOpts{
// 		Version: responseHeaderVersion,
// 	}); err != nil {
// 		return err
// 	}

// 	if !handlers.IsVersionSupported(request.ApiVersion.Key, request.ApiVersion.Version) {
// 		if err = kafka.NewEncoder(response).Encode(handlers.UnsupportedVersion); err != nil {
// 			return err
// 		}
// 	} else {
// 		handler := handlers.GetKafkaRequestHandler(request.ApiVersion.Key)

// 		if err = handler(response, request); err != nil {
// 			return err
// 		}
// 	}

// 	if err = kafka.NewEncoder(writer).Encode(response.messageSize()); err != nil {
// 		return err
// 	}

// 	if _, err = io.Copy(writer, buffer); err != nil {
// 		return err
// 	}

// 	return nil
// }
