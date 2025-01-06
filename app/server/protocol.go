package server

type ApiKey int16
type ApiVersion int16

const (
	ApiVersions             ApiKey = 18
	DescribeTopicPartitions ApiKey = 75
)

type ErrorCode int16

const (
	UnknownServerError ErrorCode = -1
	UnknownTopic       ErrorCode = 3
	UnsupportedVersion ErrorCode = 35
)

type TaggedField struct {
	Tag  uint32 `kafka:"0"`
	Data []byte `kafka:"1"`
}
