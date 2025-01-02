package apis

import (
	"io"
	"sync"

	"github.com/codecrafters-io/kafka-starter-go/app/support"
)

type ApiVersionsRequestBody struct {
	ClientId      string
	ClientVersion string
	TagBuffer     byte
}

func ParseApiVersionRequestBody(reader io.Reader) (requestBody *ApiVersionsRequestBody, err error) {
	requestBody = new(ApiVersionsRequestBody)

	if requestBody.ClientId, err = support.ReadCompactString(reader); err != nil {
		return nil, err
	}

	if requestBody.ClientVersion, err = support.ReadCompactString(reader); err != nil {
		return nil, err
	}

	if requestBody.TagBuffer, err = support.ReadByte(reader); err != nil {
		return nil, err
	}

	return requestBody, nil
}

type ApiVersionsResponseBody struct {
	ErrorCode      support.ErrorCode
	ApiKeys        []ApiKeyVersion
	ThrottleTimeMs int32
	TagBuffer      byte
}

func NewApiVersionsResponseBody() *ApiVersionsResponseBody {
	return &ApiVersionsResponseBody{
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

	if err := support.WriteByte(writer, r.TagBuffer); err != nil {
		return nil
	}

	return nil
}

type ApiKeyVersion struct {
	Key        support.ApiKey
	MinVersion int16
	MaxVersion int16
	TagBuffer  byte
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

	if err := support.WriteByte(writer, a.TagBuffer); err != nil {
		return nil
	}

	return nil
}

var supportedApiVersions map[support.ApiKey]ApiKeyVersion

var supportedApiVersionsOnce sync.Once

func initSupportedApiVersions() {
	supportedApiVersions = make(map[support.ApiKey]ApiKeyVersion)

	supportedApiVersions[support.ApiVersions] = ApiKeyVersion{
		Key:        support.ApiVersions,
		MinVersion: 0,
		MaxVersion: 4,
	}

	supportedApiVersions[support.DescribeTopicPartitions] = ApiKeyVersion{
		Key:        support.DescribeTopicPartitions,
		MinVersion: 0,
		MaxVersion: 0,
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
