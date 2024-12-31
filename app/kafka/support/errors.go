package support

import "io"

type ErrorCode int16

const (
	UNSUPPORTED_VERSION ErrorCode = 35
)

func (e ErrorCode) Write(writer io.Writer) error {
	if err := WriteInt16(writer, int16(e)); err != nil {
		return err
	}

	return nil
}
