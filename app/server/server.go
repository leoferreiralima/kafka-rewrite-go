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
)

type HandlerFunc func(ResponseWriter, *Request) error

type handlerRequestOpts struct {
	version int
}

type handlerResponseOpts struct {
	version int
}

type handlerOpts struct {
	request  handlerRequestOpts
	response handlerResponseOpts
}

type handlerState struct {
	opts        handlerOpts
	handlerFunc HandlerFunc
}

type KafkaApiId struct {
	ApiKey     ApiKey
	MinVersion ApiVersion
	MaxVersion ApiVersion
}

type KafkaServer struct {
	mutex    sync.RWMutex
	logger   *log.Logger
	handlers map[KafkaApiId]handlerState
}

func NewKafkaServer() *KafkaServer {
	return &KafkaServer{
		logger:   log.New(os.Stdout, "kafka-server:", log.LstdFlags|log.LUTC|log.Lmsgprefix|log.Lshortfile),
		handlers: make(map[KafkaApiId]handlerState),
	}
}

func (ks *KafkaServer) Handler(apiKey ApiKey) *handlerBuilder {
	return &handlerBuilder{
		server:     ks,
		apiKey:     apiKey,
		minVersion: -1,
		maxVersion: -1,
		opts: handlerOpts{
			handlerRequestOpts{
				version: 2,
			},
			handlerResponseOpts{
				version: 0,
			},
		},
		handlerFunc: nil,
	}
}

func (ks *KafkaServer) handlerFunc(
	apiKey KafkaApiId,
	handler HandlerFunc,
	opts handlerOpts,
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

	ks.handlers[apiKey] = handlerState{
		opts:        opts,
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
		ks.handleError(res, UnsupportedVersion)
		return
	}

	opts := handlerState.opts
	handler := handlerState.handlerFunc

	if err := ks.writeResponseHeaders(res, int(opts.response.version)); err != nil {
		ks.logger.Printf("Couldn't encode response headers: %s\n%v", fmt.Sprint(res.headers), err)
		ks.handleError(res, UnknownServerError)
	}

	if err := handler(res, req); err != nil {
		ks.logger.Printf("Couldn't handle request:%v", err)
		ks.handleError(res, UnknownServerError)
	}

	if err := ks.sendResponse(res); err != nil {
		ks.logger.Printf("Couldn't send response:%v", err)
		ks.handleError(res, UnknownServerError)
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

func (ks *KafkaServer) handleError(res *response, errorCode ErrorCode) {
	handlerState, found := ks.findHandler(res.req)
	version := 0
	if found {
		version = int(handlerState.opts.response.version)
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

type conn struct {
	server     *KafkaServer
	connection net.Conn
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
			c.server.handleError(response, UnknownServerError)
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

type handlerBuilder struct {
	server      *KafkaServer
	apiKey      ApiKey
	minVersion  ApiVersion
	maxVersion  ApiVersion
	opts        handlerOpts
	handlerFunc HandlerFunc
}

func (hb *handlerBuilder) Version(minVersion ApiVersion, maxVersion ApiVersion) *handlerBuilder {
	hb.minVersion = minVersion
	hb.maxVersion = maxVersion
	return hb
}

func (hb *handlerBuilder) Opts() *handlerOptsBuilder {
	return &handlerOptsBuilder{
		handlerBuilder: hb,
		requestOpts:    hb.opts.request,
		responseOpts:   hb.opts.response,
	}
}

func (hb *handlerBuilder) Add(handlerFunc HandlerFunc) {
	// TODO validate if all values is setted correctly

	apiKey := KafkaApiId{
		ApiKey:     hb.apiKey,
		MinVersion: hb.minVersion,
		MaxVersion: hb.maxVersion,
	}
	hb.server.handlerFunc(apiKey, handlerFunc, hb.opts)
}

type handlerOptsBuilder struct {
	handlerBuilder *handlerBuilder
	requestOpts    handlerRequestOpts
	responseOpts   handlerResponseOpts
}

func (ob *handlerOptsBuilder) RequestHeaderVersion(version int) *handlerOptsBuilder {
	ob.requestOpts.version = version
	return ob
}

func (ob *handlerOptsBuilder) ResponseHeaderVersion(version int) *handlerOptsBuilder {
	ob.responseOpts.version = version
	return ob
}

func (ob *handlerOptsBuilder) And() *handlerBuilder {
	ob.handlerBuilder.opts.request = ob.requestOpts
	ob.handlerBuilder.opts.response = ob.responseOpts
	return ob.handlerBuilder
}
