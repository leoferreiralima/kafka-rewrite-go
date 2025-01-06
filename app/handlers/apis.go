package handlers

import "github.com/codecrafters-io/kafka-starter-go/app/server"

const (
	ApiVersions             server.ApiKey = 18
	DescribeTopicPartitions server.ApiKey = 75
)

const (
	UnsupportedVersion server.ErrorCode = 35
	UnknownTopic       server.ErrorCode = 3
)
