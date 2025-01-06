package handlers

import (
	"fmt"
	"sync"

	"github.com/codecrafters-io/kafka-starter-go/app/encoding/kafka"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

type ApiVersionsRequest struct {
	ClientId      string                 `kafka:"0,compact"`
	ClientVersion string                 `kafka:"1,compact"`
	TaggedFields  []protocol.TaggedField `kafka:"2,compact"`
}

type ApiVersionsResponse struct {
	ErrorCode      protocol.ErrorCode     `kafka:"0"`
	ApiKeys        []ApiKeyVersion        `kafka:"1,compact"`
	ThrottleTimeMs int32                  `kafka:"2"`
	TaggedFields   []protocol.TaggedField `kafka:"3,compact,nilable"`
}

func NewApiVersionsResponseBody() *ApiVersionsResponse {
	return &ApiVersionsResponse{
		ThrottleTimeMs: 0,
	}
}

type ApiKeyVersion struct {
	Key          protocol.ApiKey        `kafka:"0"`
	MinVersion   int16                  `kafka:"1"`
	MaxVersion   int16                  `kafka:"2"`
	TaggedFields []protocol.TaggedField `kafka:"3,compact,nilable"`
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

var supportedApiVersions map[protocol.ApiKey]ApiKeyVersion

var supportedApiVersionsOnce sync.Once

func initSupportedApiVersions() {
	supportedApiVersions = make(map[protocol.ApiKey]ApiKeyVersion)

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

func GetSupportedApiVersions() map[protocol.ApiKey]ApiKeyVersion {
	supportedApiVersionsOnce.Do(initSupportedApiVersions)
	return supportedApiVersions
}

func IsVersionSupported(apiKey protocol.ApiKey, version int16) bool {
	apiKeyVersion, exists := GetSupportedApiVersions()[apiKey]

	if !exists {
		return false
	}

	if version < apiKeyVersion.MinVersion || version > apiKeyVersion.MaxVersion {
		return false
	}

	return true
}
