package main

import (
	"fmt"
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
			fmt.Println("Error parsing request: ", err.Error())
			panic(err)
		}

		fmt.Printf("Request ApiKey: %d, ApiVersion: %d\n", request.ApiKey, request.ApiVersion)

		response := requestHandler(request)

		if err = response.Write(conn); err != nil {
			fmt.Println("Error writing response: ", err.Error())
			panic(err)
		}
	}
}

func requestHandler(request *kafka.Request) (response kafka.Response) {
	response = kafka.NewResponse()
	response.CorrelationId = request.CorrelationId

	if !apis.IsVersionSupported(request.ApiKey, request.ApiVersion) {
		support.UNSUPPORTED_VERSION.Write(response.Body)
		return response
	}

	switch request.ApiKey {
	case support.API_VERSIONS:
		apiVersionsHandler(request, &response)
	}
	return response
}

func apiVersionsHandler(_ *kafka.Request, response *kafka.Response) {
	apiVersionsResponseBody := apis.NewApiVersionsResponseBody()

	supportedApiKeys := apis.GetSupportedApiVersions()

	for _, apiKey := range supportedApiKeys {
		apiVersionsResponseBody.ApiKeys = append(apiVersionsResponseBody.ApiKeys, apiKey)
	}

	apiVersionsResponseBody.Write(response.Body)
}
