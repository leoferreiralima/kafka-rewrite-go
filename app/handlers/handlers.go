package handlers

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

type KafkaRequestHandlerFunc func(response server.ResponseWriter, request *server.Request) error

func GetKafkaRequestHandler(apiKey protocol.ApiKey) KafkaRequestHandlerFunc {
	switch apiKey {
	case ApiVersions:
		return ApiVersionsHandler
	case DescribeTopicPartitions:
		return DescribeTopicPartitionsHandler
	}

	return func(_ server.ResponseWriter, _ *server.Request) error {
		return fmt.Errorf("handler not found")
	}
}
