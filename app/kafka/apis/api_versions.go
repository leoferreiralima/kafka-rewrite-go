package apis

import (
	"io"
	"sync"

	"github.com/codecrafters-io/kafka-starter-go/app/kafka/support"
)

type ApiVersionsResponseBody struct {
	ErrorCode      support.ErrorCode
	ApiKeys        []ApiKeyVersion
	ThrottleTimeMs int32
}

func NewApiVersionsResponseBody() ApiVersionsResponseBody {
	return ApiVersionsResponseBody{
		ThrottleTimeMs: 0,
	}
}

func (r *ApiVersionsResponseBody) Write(writer io.Writer) error {

	if err := support.WriteInt16(writer, int16(r.ErrorCode)); err != nil {
		return err
	}

	if err := support.WriteInt8(writer, int8(len(r.ApiKeys)+1)); err != nil {
		return nil
	}

	for _, apiKey := range r.ApiKeys {
		if err := apiKey.Write(writer); err != nil {
			return err
		}
	}

	if err := support.WriteInt32(writer, r.ThrottleTimeMs); err != nil {
		return err
	}

	if err := support.WriteInt8(writer, 0); err != nil {
		return nil
	}

	return nil
}

type ApiKeyVersion struct {
	Key        support.ApiKey
	MinVersion int16
	MaxVersion int16
}

func (a *ApiKeyVersion) Write(writer io.Writer) error {

	if err := support.WriteInt16(writer, int16(a.Key)); err != nil {
		return err
	}

	if err := support.WriteInt16(writer, a.MinVersion); err != nil {
		return err
	}

	if err := support.WriteInt16(writer, a.MaxVersion); err != nil {
		return err
	}

	if err := support.WriteInt8(writer, 0); err != nil {
		return nil
	}

	return nil
}

var supportedApiVersions map[support.ApiKey]ApiKeyVersion

var supportedApiVersionsOnce sync.Once

func initSupportedApiVersions() {
	supportedApiVersions = make(map[support.ApiKey]ApiKeyVersion)

	supportedApiVersions[support.API_VERSIONS] = ApiKeyVersion{
		Key:        support.API_VERSIONS,
		MinVersion: 0,
		MaxVersion: 4,
	}
}

func GetSupportedApiVersions() map[support.ApiKey]ApiKeyVersion {
	supportedApiVersionsOnce.Do(initSupportedApiVersions)
	return supportedApiVersions
}

func IsVersionSupported(apiKey support.ApiKey, version int16) bool {
	apiKeyVersion, exists := GetSupportedApiVersions()[apiKey]

	if !exists {
		return false
	}

	if version < apiKeyVersion.MinVersion || version > apiKeyVersion.MaxVersion {
		return false
	}

	return true
}
