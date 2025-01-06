package handlers

import "github.com/codecrafters-io/kafka-starter-go/app/protocol"

const (
	ApiVersions             protocol.ApiKey = 18
	DescribeTopicPartitions protocol.ApiKey = 75
)

const (
	UnsupportedVersion protocol.ErrorCode = 35
	UnknownTopic       protocol.ErrorCode = 3
)
