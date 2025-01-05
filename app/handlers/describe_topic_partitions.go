package handlers

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/encoding/kafka"
	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

type DescribeTopicPartitionsCursor struct {
	Topic          string        `kafka:"0"`
	PartitionIndex int32         `kafka:"1"`
	TaggedFields   []TaggedField `kafka:"2,compact,nilable"`
}

type DescribeTopicPartitionsRequest struct {
	Topics []struct {
		Name         string        `kafka:"0,compact"`
		TaggedFields []TaggedField `kafka:"1,compact"`
	} `kafka:"0,compact"`
	ResponsePartionLimit int32                          `kafka:"1"`
	Cursor               *DescribeTopicPartitionsCursor `kafka:"2,nilable"`
	TaggedFields         []TaggedField                  `kafka:"3,compact,nilable"`
}

type DescribeTopicPartitionsResponse struct {
	ThrottleTimeMs int32                          `kafka:"1"`
	Topics         []PartitionsTopicsResponseBody `kafka:"2,compact"`
	NextCursor     *DescribeTopicPartitionsCursor `kafka:"3,nilable"`
	TaggedFields   []TaggedField                  `kafka:"4,compact,nilable"`
}

func NewDescribeTopicPartitionsResponse() *DescribeTopicPartitionsResponse {
	return &DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
	}
}

type PartitionsTopicsResponseBody struct {
	ErrorCode            ErrorCode                        `kafka:"0"`
	Name                 string                           `kafka:"1,compact"`
	Id                   [16]byte                         `kafka:"2,raw"`
	IsInternal           bool                             `kafka:"3"`
	Partitions           []DescribePartitionsResponseBody `kafka:"4,compact"`
	AuthorizedOperations int32                            `kafka:"5"`
	TaggedFields         []TaggedField                    `kafka:"6,compact,nilable"`
}

type DescribePartitionsResponseBody struct{}

func DescribeTopicPartitionsHandler(responseWriter server.ResponseWriter, request *server.Request) (err error) {
	var requestData DescribeTopicPartitionsRequest

	if err = kafka.NewDecoder(request.Body).Decode(&requestData); err != nil {
		fmt.Println(requestData)
		return err
	}

	fmt.Printf("Topics: %s\nResponsePartionLimit: %d\n", fmt.Sprint(requestData.Topics), requestData.ResponsePartionLimit)

	responseBody := NewDescribeTopicPartitionsResponse()

	for _, topic := range requestData.Topics {
		topicResponse := PartitionsTopicsResponseBody{
			ErrorCode:            UnknownTopic,
			Name:                 topic.Name,
			IsInternal:           false,
			AuthorizedOperations: 0b0000_1101_1111_1000,
		}

		responseBody.Topics = append(responseBody.Topics, topicResponse)
	}

	return kafka.NewEncoder(responseWriter).Encode(responseBody)
}
