package apis

import (
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/codecrafters-io/kafka-starter-go/app/support"
)

type DescribeTopicPartitionsRequest struct {
	Topics []struct {
		Name         string                 `kafka:"0,compact"`
		TaggedFields []protocol.TaggedField `kafka:"1,compact"`
	} `kafka:"0,compact"`
	ResponsePartionLimit int32 `kafka:"1"`
	Cursor               struct {
		Topic          string `kafka:"0"`
		PartitionIndex int32  `kafka:"1"`
	} `kafka:"2,nilable"`
	TaggedFields []protocol.TaggedField `kafka:"3,compact"`
}

type DescribeTopicPartitionsResponseBody struct {
	ThrottleTimeMs int32
	Topics         []*PartitionsTopicsResponseBody
	NextCursor     byte
	TagBuffer      byte
}

func NewDescribeTopicPartitionsResponseBody() *DescribeTopicPartitionsResponseBody {
	return &DescribeTopicPartitionsResponseBody{
		ThrottleTimeMs: 0,
		NextCursor:     0xff,
	}
}

func (r *DescribeTopicPartitionsResponseBody) Write(writer io.Writer) error {
	if err := support.WriteByte(writer, r.TagBuffer); err != nil {
		return err
	}

	if err := support.WriteInt32(writer, r.ThrottleTimeMs); err != nil {
		return err
	}

	if err := support.WriteCompactArray(writer, r.Topics); err != nil {
		return err
	}

	if err := support.WriteByte(writer, r.NextCursor); err != nil {
		return err
	}
	if err := support.WriteByte(writer, r.TagBuffer); err != nil {
		return err
	}

	return nil
}

type PartitionsTopicsResponseBody struct {
	ErrorCode            ErrorCode
	Name                 string
	Id                   [16]byte
	IsInternal           bool
	Partitions           []*DescribePartitionsResponseBody
	AuthorizedOperations int32
	TagBuffer            byte
}

func (r *PartitionsTopicsResponseBody) Write(writer io.Writer) error {
	if err := support.WriteInt16(writer, int16(r.ErrorCode)); err != nil {
		return err
	}

	if err := support.WriteCompactString(writer, r.Name); err != nil {
		return err
	}

	if err := support.WriteByteArray(writer, r.Id[:]); err != nil {
		return err
	}

	if err := support.WriteBool(writer, r.IsInternal); err != nil {
		return err
	}

	if err := support.WriteCompactArray(writer, r.Partitions); err != nil {
		return err
	}

	if err := support.WriteInt32(writer, r.AuthorizedOperations); err != nil {
		return err
	}

	if err := support.WriteByte(writer, r.TagBuffer); err != nil {
		return err
	}

	return nil
}

type DescribePartitionsResponseBody struct{}

func (r *DescribePartitionsResponseBody) Write(writer io.Writer) error {
	return nil
}
