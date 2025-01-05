package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/app/encoding/kafka"
	"github.com/codecrafters-io/kafka-starter-go/app/server/apis"
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

type KafkaRequestHandlerFunc func(response ResponseWriter, request *Request) error

func requestHandler(request *Request, writer io.Writer) (err error) {
	buffer := new(bytes.Buffer)
	response := &response{
		writer: buffer,
	}

	response.header.CorrelationId = request.Headers.CorrelationId

	responseHeaderVersion := 0

	if request.ApiVersion.Key == apis.DescribeTopicPartitions {
		responseHeaderVersion = 1
	}

	if err = kafka.NewEncoder(response).EncodeWithOpts(response.header, &kafka.EncoderOpts{
		Version: responseHeaderVersion,
	}); err != nil {
		return err
	}

	if !apis.IsVersionSupported(request.ApiVersion.Key, request.ApiVersion.Version) {
		if err = kafka.NewEncoder(response).Encode(apis.UnsupportedVersion); err != nil {
			return err
		}
	} else {
		handler := getKafkaRequestHandler(request.ApiVersion.Key)

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

func getKafkaRequestHandler(apiKey apis.ApiKey) KafkaRequestHandlerFunc {
	switch apiKey {
	case apis.ApiVersions:
		return apiVersionsHandler
	case apis.DescribeTopicPartitions:
		return describeTopicPartitionsHandler
	}

	return func(_ ResponseWriter, _ *Request) error {
		return fmt.Errorf("handler not found")
	}
}

func apiVersionsHandler(responseWriter ResponseWriter, request *Request) (err error) {
	var requestData apis.ApiVersionsRequest

	if err = kafka.NewDecoder(request.Body).Decode(&requestData); err != nil {
		return err
	}

	fmt.Printf("ClientName: %s, ClientVersion: %s\n", requestData.ClientId, requestData.ClientVersion)

	responseBody := apis.NewApiVersionsResponseBody()

	supportedApiKeys := apis.GetSupportedApiVersions()

	for _, apiKey := range supportedApiKeys {
		responseBody.ApiKeys = append(responseBody.ApiKeys, apiKey)
	}

	return kafka.NewEncoder(responseWriter).Encode(responseBody)
}

func describeTopicPartitionsHandler(responseWriter ResponseWriter, request *Request) (err error) {
	var requestData apis.DescribeTopicPartitionsRequest

	if err = kafka.NewDecoder(request.Body).Decode(&requestData); err != nil {
		fmt.Println(requestData)
		return err
	}

	fmt.Printf("Topics: %s\nResponsePartionLimit: %d\n", fmt.Sprint(requestData.Topics), requestData.ResponsePartionLimit)

	responseBody := apis.NewDescribeTopicPartitionsResponse()

	for _, topic := range requestData.Topics {
		topicResponse := apis.PartitionsTopicsResponseBody{
			ErrorCode:            apis.UnknownTopic,
			Name:                 topic.Name,
			IsInternal:           false,
			AuthorizedOperations: 0b0000_1101_1111_1000,
		}

		responseBody.Topics = append(responseBody.Topics, topicResponse)
	}

	return kafka.NewEncoder(responseWriter).Encode(responseBody)
}
