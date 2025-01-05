package handlers

type ApiKey int16

const (
	ApiVersions             ApiKey = 18
	DescribeTopicPartitions ApiKey = 75
)

type ErrorCode int16

const (
	UnsupportedVersion ErrorCode = 35
	UnknownTopic       ErrorCode = 3
)

type TaggedField struct {
	Tag  uint32 `kafka:"0"`
	Data []byte `kafka:"1"`
}
