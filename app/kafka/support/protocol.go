package support

import (
	"encoding/binary"
	"io"
)

func WriteInt8(writer io.Writer, value int8) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}

func ReadInt16(reader io.Reader) (int16, error) {
	var value int16
	err := binary.Read(reader, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func WriteInt16(writer io.Writer, value int16) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}

func ReadInt32(reader io.Reader) (int32, error) {
	var value int32
	err := binary.Read(reader, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func WriteInt32(writer io.Writer, value int32) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}
