package main

import (
	"fmt"
	"io"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka"
	"github.com/codecrafters-io/kafka-starter-go/app/kafka/apis"
	"github.com/codecrafters-io/kafka-starter-go/app/kafka/support"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleNewConnection(conn)
	}

}

func handleNewConnection(conn net.Conn) {
	defer conn.Close()

	for {
		request, err := kafka.ParseRequest(conn)

		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection ended")
				break
			}
			panic(err)
		}

		fmt.Printf("Request ApiKey: %d, ApiVersion: %d\n", request.ApiKey, request.ApiVersion)

		response, err := requestHandler(request)

		if err != nil {
			fmt.Println("Error on handle request: ", err.Error())
			break
		}

		if err = response.Write(conn); err != nil {
			fmt.Println("Error writing response: ", err.Error())
			break
		}
	}
}

type KafkaRequestHandlerFunc func(request *kafka.Request, response *kafka.Response) error

func requestHandler(request *kafka.Request) (response kafka.Response, err error) {
	response = kafka.NewResponse()
	response.CorrelationId = request.CorrelationId

	if !apis.IsVersionSupported(request.ApiKey, request.ApiVersion) {
		support.UNSUPPORTED_VERSION.Write(response.Body)
		return response, nil
	}

	handler := getKafkaRequestHandler(request.ApiKey)

	if err = handler(request, &response); err != nil {
		return response, err
	}
	return response, nil
}

func getKafkaRequestHandler(apiKey support.ApiKey) KafkaRequestHandlerFunc {
	switch apiKey {
	case support.API_VERSIONS:
		return apiVersionsHandler
	case support.DESCRIBE_TOPIC_PARTITIONS:
		return describeTopicPartitionsHandler
	}

	return func(_ *kafka.Request, _ *kafka.Response) error {
		return fmt.Errorf("handler not found")
	}
}

func apiVersionsHandler(request *kafka.Request, response *kafka.Response) error {
	requestBody, err := apis.ParseApiVersionRequestBody(request.Body)

	if err != nil {
		return err
	}

	fmt.Printf("ClientName: %s, ClientVersion: %s\n", requestBody.ClientId, requestBody.ClientVersion)

	responseBody := apis.NewApiVersionsResponseBody()

	supportedApiKeys := apis.GetSupportedApiVersions()

	for _, apiKey := range supportedApiKeys {
		responseBody.ApiKeys = append(responseBody.ApiKeys, apiKey)
	}

	responseBody.Write(response.Body)

	return nil
}

func describeTopicPartitionsHandler(request *kafka.Request, response *kafka.Response) error {
	requestBody, err := apis.ParseDescribeTopicPartitionsRequestBody(request.Body)

	if err != nil {
		return err
	}

	fmt.Printf("Topics: %s\nResponsePartionLimit: %d\n", requestBody.Topics, requestBody.ResponsePartionLimit)

	responseBody := apis.NewDescribeTopicPartitionsResponseBody()

	for _, topic := range requestBody.Topics {
		topicResponse := &apis.PartitionsTopicsResponseBody{
			ErrorCode:            support.UNKNOWN_TOPIC,
			Name:                 topic,
			IsInternal:           false,
			AuthorizedOperations: 0b0000_1101_1111_1000,
		}

		responseBody.Topics = append(responseBody.Topics, topicResponse)
	}

	responseBody.Write(response.Body)

	return nil
}
