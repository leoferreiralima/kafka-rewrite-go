package apis

import (
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

type DescribeTopicPartitionsCursor struct {
	Topic          string                 `kafka:"0"`
	PartitionIndex int32                  `kafka:"1"`
	TaggedFields   []protocol.TaggedField `kafka:"2,compact,nilable"`
}

type DescribeTopicPartitionsRequest struct {
	Topics []struct {
		Name         string                 `kafka:"0,compact"`
		TaggedFields []protocol.TaggedField `kafka:"1,compact"`
	} `kafka:"0,compact"`
	ResponsePartionLimit int32                          `kafka:"1"`
	Cursor               *DescribeTopicPartitionsCursor `kafka:"2,nilable"`
	TaggedFields         []protocol.TaggedField         `kafka:"3,compact,nilable"`
}

type DescribeTopicPartitionsResponse struct {
	ThrottleTimeMs int32                          `kafka:"1"`
	Topics         []PartitionsTopicsResponseBody `kafka:"2,compact"`
	NextCursor     *DescribeTopicPartitionsCursor `kafka:"3,nilable"`
	TaggedFields   []protocol.TaggedField         `kafka:"4,compact,nilable"`
}

func NewDescribeTopicPartitionsResponse() *DescribeTopicPartitionsResponse {
	return &DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
	}
}

// func (r *DescribeTopicPartitionsResponse) Write(writer io.Writer) error {
// 	if err := support.WriteByte(writer, r.TagBuffer); err != nil {
// 		return err
// 	}

// 	if err := support.WriteInt32(writer, r.ThrottleTimeMs); err != nil {
// 		return err
// 	}

// 	if err := support.WriteCompactArray(writer, r.Topics); err != nil {
// 		return err
// 	}

// 	if err := support.WriteByte(writer, r.NextCursor); err != nil {
// 		return err
// 	}
// 	if err := support.WriteByte(writer, r.TagBuffer); err != nil {
// 		return err
// 	}

// 	return nil
// }

type PartitionsTopicsResponseBody struct {
	ErrorCode            ErrorCode                        `kafka:"0"`
	Name                 string                           `kafka:"1,compact"`
	Id                   [16]byte                         `kafka:"2,raw"`
	IsInternal           bool                             `kafka:"3"`
	Partitions           []DescribePartitionsResponseBody `kafka:"4,compact"`
	AuthorizedOperations int32                            `kafka:"5"`
	TaggedFields         []protocol.TaggedField           `kafka:"6,compact,nilable"`
}

// func (r *PartitionsTopicsResponseBody) Write(writer io.Writer) error {
// 	if err := support.WriteInt16(writer, int16(r.ErrorCode)); err != nil {
// 		return err
// 	}

// 	if err := support.WriteCompactString(writer, r.Name); err != nil {
// 		return err
// 	}

// 	if err := support.WriteByteArray(writer, r.Id[:]); err != nil {
// 		return err
// 	}

// 	if err := support.WriteBool(writer, r.IsInternal); err != nil {
// 		return err
// 	}

// 	if err := support.WriteCompactArray(writer, r.Partitions); err != nil {
// 		return err
// 	}

// 	if err := support.WriteInt32(writer, r.AuthorizedOperations); err != nil {
// 		return err
// 	}

// 	if err := support.WriteByte(writer, r.TagBuffer); err != nil {
// 		return err
// 	}

// 	return nil
// }

type DescribePartitionsResponseBody struct{}

func (r *DescribePartitionsResponseBody) Write(writer io.Writer) error {
	return nil
}
