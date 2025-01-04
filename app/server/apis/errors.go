package apis

import (
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/support"
)

type ErrorCode int16

const (
	UnsupportedVersion ErrorCode = 35
	UnknownTopic       ErrorCode = 3
)

func (e ErrorCode) Write(writer io.Writer) error {
	if err := support.WriteInt16(writer, int16(e)); err != nil {
		return err
	}

	return nil
}
