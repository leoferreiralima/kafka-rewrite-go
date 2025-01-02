package support

import "io"

type ErrorCode int16

const (
	UnsupportedVersion ErrorCode = 35
	UnknownTopic       ErrorCode = 3
)

func (e ErrorCode) Write(writer io.Writer) error {
	if err := WriteInt16(writer, int16(e)); err != nil {
		return err
	}

	return nil
}
