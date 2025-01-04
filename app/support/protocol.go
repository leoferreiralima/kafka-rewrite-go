package support

import (
	"encoding/binary"
	"io"
)

type Writer interface {
	Write(writer io.Writer) error
}

func WriteInt8(writer io.Writer, value int8) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}

func WriteInt16(writer io.Writer, value int16) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}

func WriteInt32(writer io.Writer, value int32) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}

func WriteString(writer io.Writer, value string) (err error) {
	length := int16(len(value)) + 1

	if err = WriteInt16(writer, length); err != nil {
		return err
	}

	if err = WriteByteArray(writer, []byte(value)); err != nil {
		return nil
	}

	return nil
}

func WriteCompactString(writer io.Writer, value string) (err error) {
	length := int8(len(value)) + 1

	if err = WriteInt8(writer, length); err != nil {
		return err
	}

	if err = WriteByteArray(writer, []byte(value)); err != nil {
		return nil
	}

	return nil
}

func WriteByte(writer io.Writer, value byte) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}

func WriteByteArray(writer io.Writer, bytes []byte) (err error) {
	if _, err = writer.Write(bytes); err != nil {
		return err
	}

	return nil
}

func WriteBool(writer io.Writer, value bool) (err error) {
	if value {
		if err = WriteByte(writer, 1); err != nil {
			return err
		}
	} else {
		if err = WriteByte(writer, 0); err != nil {
			return err
		}
	}

	return nil
}

func WriteArray(writer io.Writer, items []Writer) (err error) {
	lenght := int32(len(items)) + 1

	if err = WriteInt32(writer, lenght); err != nil {
		return err
	}

	for _, item := range items {
		if err = item.Write(writer); err != nil {
			return err
		}
	}

	return nil
}

func WriteCompactArray[W Writer](writer io.Writer, items []W) (err error) {
	lenght := int8(len(items)) + 1

	if err = WriteInt8(writer, lenght); err != nil {
		return err
	}

	for _, item := range items {
		if err = item.Write(writer); err != nil {
			return err
		}
	}

	return nil
}
