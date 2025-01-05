package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/app/encoding/kafka"
	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
)

func HandleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		request, err := ParseRequest(conn)

		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection ended")
				break
			}
			panic(err)
		}

		fmt.Printf("Request ApiKey: %d, ApiVersion: %d\n", request.ApiVersion.Key, request.ApiVersion.Version)
		writer := bufio.NewWriter(conn)
		err = requestHandler(request, writer)

		if err != nil {
			fmt.Println("Error on handle request: ", err.Error())
			break
		}

		if err = writer.Flush(); err != nil {
			fmt.Println("Error on handle request: ", err.Error())
			break
		}
	}
}

func requestHandler(request *Request, writer io.Writer) (err error) {
	buffer := new(bytes.Buffer)
	response := &response{
		writer: buffer,
	}

	response.header.CorrelationId = request.Headers.CorrelationId

	responseHeaderVersion := 0

	if request.ApiVersion.Key == handlers.DescribeTopicPartitions {
		responseHeaderVersion = 1
	}

	if err = kafka.NewEncoder(response).EncodeWithOpts(response.header, &kafka.EncoderOpts{
		Version: responseHeaderVersion,
	}); err != nil {
		return err
	}

	if !handlers.IsVersionSupported(request.ApiVersion.Key, request.ApiVersion.Version) {
		if err = kafka.NewEncoder(response).Encode(handlers.UnsupportedVersion); err != nil {
			return err
		}
	} else {
		handler := handlers.GetKafkaRequestHandler(request.ApiVersion.Key)

		if err = handler(response, request); err != nil {
			return err
		}
	}

	if err = kafka.NewEncoder(writer).Encode(response.messageSize()); err != nil {
		return err
	}

	if _, err = io.Copy(writer, buffer); err != nil {
		return err
	}

	return nil
}
