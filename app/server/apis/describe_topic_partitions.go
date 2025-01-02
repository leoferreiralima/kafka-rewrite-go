package apis

import (
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/support"
)

type DescribeTopicPartitionsRequestBody struct {
	Topics               []string
	ResponsePartionLimit int32
	Cursor               struct {
		Topic          string
		PartitionIndex int32
	}
	TagBuffer byte
}

func ParseDescribeTopicPartitionsRequestBody(reader io.Reader) (requestBody *DescribeTopicPartitionsRequestBody, err error) {
	requestBody = new(DescribeTopicPartitionsRequestBody)

	if requestBody.Topics, err = support.ReadCompactArray(reader, support.ReadCompactString); err != nil {
		return nil, err
	}

	if requestBody.ResponsePartionLimit, err = support.ReadInt32(reader); err != nil {
		return nil, err
	}

	if _, err = support.ReadByte(reader); err != nil {
		return nil, err
	}

	if requestBody.TagBuffer, err = support.ReadByte(reader); err != nil {
		return nil, err
	}

	return requestBody, nil
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
	ErrorCode            support.ErrorCode
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
