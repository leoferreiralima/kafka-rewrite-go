package handlers

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/encoding/kafka"
	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

type ApiVersionsRequest struct {
	ClientId      string               `kafka:"0,compact"`
	ClientVersion string               `kafka:"1,compact"`
	TaggedFields  []server.TaggedField `kafka:"2,compact"`
}

type ApiVersionsResponse struct {
	ErrorCode      server.ErrorCode     `kafka:"0"`
	ApiKeys        []ApiKeyVersion      `kafka:"1,compact"`
	ThrottleTimeMs int32                `kafka:"2"`
	TaggedFields   []server.TaggedField `kafka:"3,compact,nilable"`
}

type ApiKeyVersion struct {
	Key          server.ApiKey        `kafka:"0"`
	MinVersion   server.ApiVersion    `kafka:"1"`
	MaxVersion   server.ApiVersion    `kafka:"2"`
	TaggedFields []server.TaggedField `kafka:"3,compact,nilable"`
}

func ApiVersionsHandler(responseWriter server.ResponseWriter, request *server.Request) (err error) {
	var requestData ApiVersionsRequest

	if err = kafka.NewDecoder(request.Body).Decode(&requestData); err != nil {
		return err
	}

	fmt.Printf("ClientName: %s, ClientVersion: %s\n", requestData.ClientId, requestData.ClientVersion)

	responseBody := &ApiVersionsResponse{
		ThrottleTimeMs: 0,
	}

	supportedApis := server.GetSupportedApis()

	for apiKey, rangeVersion := range supportedApis {
		responseBody.ApiKeys = append(responseBody.ApiKeys, ApiKeyVersion{
			Key:        apiKey,
			MinVersion: rangeVersion.Min,
			MaxVersion: rangeVersion.Max,
		})
	}

	return kafka.NewEncoder(responseWriter).Encode(responseBody)
}
