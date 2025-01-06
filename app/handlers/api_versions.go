package handlers

import (
	"fmt"
	"sync"

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

func NewApiVersionsResponseBody() *ApiVersionsResponse {
	return &ApiVersionsResponse{
		ThrottleTimeMs: 0,
	}
}

type ApiKeyVersion struct {
	Key          server.ApiKey        `kafka:"0"`
	MinVersion   int16                `kafka:"1"`
	MaxVersion   int16                `kafka:"2"`
	TaggedFields []server.TaggedField `kafka:"3,compact,nilable"`
}

func ApiVersionsHandler(responseWriter server.ResponseWriter, request *server.Request) (err error) {
	var requestData ApiVersionsRequest

	if err = kafka.NewDecoder(request.Body).Decode(&requestData); err != nil {
		return err
	}

	fmt.Printf("ClientName: %s, ClientVersion: %s\n", requestData.ClientId, requestData.ClientVersion)

	responseBody := NewApiVersionsResponseBody()

	supportedApiKeys := GetSupportedApiVersions()

	for _, apiKey := range supportedApiKeys {
		responseBody.ApiKeys = append(responseBody.ApiKeys, apiKey)
	}

	return kafka.NewEncoder(responseWriter).Encode(responseBody)
}

var supportedApiVersions map[server.ApiKey]ApiKeyVersion

var supportedApiVersionsOnce sync.Once

func initSupportedApiVersions() {
	supportedApiVersions = make(map[server.ApiKey]ApiKeyVersion)

	supportedApiVersions[ApiVersions] = ApiKeyVersion{
		Key:        ApiVersions,
		MinVersion: 0,
		MaxVersion: 4,
	}

	supportedApiVersions[DescribeTopicPartitions] = ApiKeyVersion{
		Key:        DescribeTopicPartitions,
		MinVersion: 0,
		MaxVersion: 0,
	}
}

func GetSupportedApiVersions() map[server.ApiKey]ApiKeyVersion {
	supportedApiVersionsOnce.Do(initSupportedApiVersions)
	return supportedApiVersions
}

func IsVersionSupported(apiKey server.ApiKey, version int16) bool {
	apiKeyVersion, exists := GetSupportedApiVersions()[apiKey]

	if !exists {
		return false
	}

	if version < apiKeyVersion.MinVersion || version > apiKeyVersion.MaxVersion {
		return false
	}

	return true
}
