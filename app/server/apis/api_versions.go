package apis

import (
	"sync"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

type ApiVersionsRequest struct {
	ClientId      string                 `kafka:"0,compact"`
	ClientVersion string                 `kafka:"1,compact"`
	TaggedFields  []protocol.TaggedField `kafka:"2,compact"`
}

type ApiVersionsResponse struct {
	ErrorCode      ErrorCode              `kafka:"0"`
	ApiKeys        []ApiKeyVersion        `kafka:"1,compact"`
	ThrottleTimeMs int32                  `kafka:"2"`
	TaggedFields   []protocol.TaggedField `kafka:"3,compact,nilable"`
}

func NewApiVersionsResponseBody() *ApiVersionsResponse {
	return &ApiVersionsResponse{
		ThrottleTimeMs: 0,
	}
}

// func (r *ApiVersionsResponse) Write(writer io.Writer) error {

// 	if err := support.WriteInt16(writer, int16(r.ErrorCode)); err != nil {
// 		return err
// 	}

// 	if err := support.WriteInt8(writer, int8(len(r.ApiKeys)+1)); err != nil {
// 		return nil
// 	}

// 	for _, apiKey := range r.ApiKeys {
// 		if err := apiKey.Write(writer); err != nil {
// 			return err
// 		}
// 	}

// 	if err := support.WriteInt32(writer, r.ThrottleTimeMs); err != nil {
// 		return err
// 	}

// 	if err := support.WriteByte(writer, r.TagBuffer); err != nil {
// 		return nil
// 	}

// 	return nil
// }

type ApiKeyVersion struct {
	Key          ApiKey                 `kafka:"0"`
	MinVersion   int16                  `kafka:"1"`
	MaxVersion   int16                  `kafka:"2"`
	TaggedFields []protocol.TaggedField `kafka:"3,compact,nilable"`
}

// func (a *ApiKeyVersion) Write(writer io.Writer) error {

// 	if err := support.WriteInt16(writer, int16(a.Key)); err != nil {
// 		return err
// 	}

// 	if err := support.WriteInt16(writer, a.MinVersion); err != nil {
// 		return err
// 	}

// 	if err := support.WriteInt16(writer, a.MaxVersion); err != nil {
// 		return err
// 	}

// 	if err := support.WriteByte(writer, a.TagBuffer); err != nil {
// 		return nil
// 	}

// 	return nil
// }

var supportedApiVersions map[ApiKey]ApiKeyVersion

var supportedApiVersionsOnce sync.Once

func initSupportedApiVersions() {
	supportedApiVersions = make(map[ApiKey]ApiKeyVersion)

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

func GetSupportedApiVersions() map[ApiKey]ApiKeyVersion {
	supportedApiVersionsOnce.Do(initSupportedApiVersions)
	return supportedApiVersions
}

func IsVersionSupported(apiKey ApiKey, version int16) bool {
	apiKeyVersion, exists := GetSupportedApiVersions()[apiKey]

	if !exists {
		return false
	}

	if version < apiKeyVersion.MinVersion || version > apiKeyVersion.MaxVersion {
		return false
	}

	return true
}
